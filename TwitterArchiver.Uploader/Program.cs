using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using System.Data.Common;
using System.Dynamic;
using System.Text;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;

using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;

namespace TwitterArchiver.Uploader
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config.json")
                .Build();

            var loggerFactory = new LoggerFactory();
            loggerFactory.AddConsole(config.GetSection("Logging"));

            var logger = loggerFactory.CreateLogger("Program");

            var container = new ServiceCollection();
            var serviceProvider = container
                .AddSingleton<ILoggerFactory>(loggerFactory)
                .AddSingleton(logger)
                .AddSingleton(RabbitMqConfig.LoadConfiguration(config))
                .AddTransient<NpgsqlConnection>((_) =>
                {
                    var connectionString = config.GetSection("Database")["ConnectionString"];
                    var connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    return connection;
                })
                .AddScoped((sp) =>
                {
                    var rabbitConfig = sp.GetRequiredService<RabbitMqConfig>();
                    var connectionFactory = new ConnectionFactory();
                    connectionFactory.UserName = rabbitConfig.Username;
                    connectionFactory.Password = rabbitConfig.Password;
                    connectionFactory.Uri = rabbitConfig.Uri;
                    return connectionFactory.CreateConnection();
                })
                .AddScoped((sp) =>
                {
                    var connection = sp.GetRequiredService<IConnection>();
                    return connection.CreateModel();
                })
                .AddScoped((sp) =>
                {
                    var prefetchSize = Convert.ToUInt16(config.GetSection("RabbitMq")["PrefertchSize"]);
                    var channel = sp.GetRequiredService<IModel>();
                    channel.BasicQos(0, prefetchSize, false);
                    return new EventingBasicConsumer(channel);
                })
                .BuildServiceProvider();

            using (serviceProvider.CreateScope())
            {
                var rabbitConfig = serviceProvider.GetRequiredService<RabbitMqConfig>();
                var queueConsumer = serviceProvider.GetRequiredService<EventingBasicConsumer>();
                var channel = serviceProvider.GetRequiredService<IModel>();
                queueConsumer.Received += CreateMessageHandler(serviceProvider, channel, logger);

                var consumerTag = channel.BasicConsume(rabbitConfig.Queue, false, queueConsumer);
                logger.LogInformation($"Registered consumer {consumerTag}");
                Console.ReadLine();
            }
        }

        private static string ReadText(byte[] message, Encoding encoding = null)
        {
            encoding = encoding ?? Encoding.UTF8;
            return encoding.GetString(message);
        }

        private static EventHandler<BasicDeliverEventArgs> CreateMessageHandler(IServiceProvider serviceProvider, IModel channel, ILogger logger)
        {
            return async (o, ea) =>
            {
                using (serviceProvider.CreateScope())
                {
                    var body = ReadText(ea.Body);
                    var screenName = ReadText(ea.BasicProperties.Headers["screenName"] as byte[]);
                    dynamic data = await Task.Factory.StartNew(() => JsonConvert.DeserializeObject<ExpandoObject>(body));
                    using (var connection = serviceProvider.GetRequiredService<NpgsqlConnection>())
                    using (var command = new NpgsqlCommand(@"
                            SELECT COUNT(*) FROM raw_tweets WHERE tweet_id = @tweetId;
                            INSERT INTO raw_tweets (tweet_id, screen_name, content) VALUES (@tweetId, @screenName, @tweetContent) 
                                ON CONFLICT(tweet_id) DO NOTHING
                        ", connection
                    ))
                    {
                        command.Parameters.AddWithValue("tweetId", data.id);
                        command.Parameters.AddWithValue("screenName", screenName);
                        command.Parameters.AddWithValue("tweetContent", NpgsqlTypes.NpgsqlDbType.Jsonb, body);

                        var result = await command.ExecuteScalarAsync();
                        var existed = Convert.ToInt32(result) != 0;

                        if (existed)
                        {
                            logger.LogInformation($"Tweet {data.id} already exists");
                        }
                        else
                        {
                            logger.LogInformation($"Tweet {data.id} has been uploaded");
                        }
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                }
            };
        }
    }
}