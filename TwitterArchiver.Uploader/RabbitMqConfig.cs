using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace TwitterArchiver.Uploader
{
    public class RabbitMqConfig
    {
        public string Uri { get; }
        public string Queue { get; }
        public string Username { get; }
        public string Password { get; }

        public RabbitMqConfig(
            string uri = "amqp://localhost/",
            string queue = "tweet.store",
            string username = null,
            string password = null
        )
        {
            Uri = uri;
            Queue = queue;
            Username = username;
            Password = password;
        }

        public static RabbitMqConfig LoadConfiguration(IConfigurationRoot config)
        {

            var section = config.GetSection("RabbitMq");
            if (section == null)
            {
                return new RabbitMqConfig();
            }
            return new RabbitMqConfig(
                section["Uri"],
                section["Queue"],
                section["Username"],
                section["Password"]
            );
        }
    }


}
