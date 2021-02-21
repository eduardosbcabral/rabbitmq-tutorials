using System;
using System.Text;
using RabbitMQ.Client;

namespace NewTask
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using(var channel = connection.CreateModel())
            {
                channel.QueueDeclare(
                    queue: "hello-queue",
                    durable: false,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                var message = GetMessage(args);

                var body = Encoding.UTF8.GetBytes(message);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true; // Marca a mensagem para ser persistida e sobreviver ao reinício de um nó RabbitMQ

                channel.BasicPublish(
                    exchange: "",
                    routingKey: "task-queue",
                    basicProperties: properties,
                    body: body);

                Console.WriteLine($" [x] Sent {message}");
            }
            
            // Console.WriteLine(" Press [enter] to exit.");
            // Console.ReadLine();
        }

        static string GetMessage(string[] args) 
            => ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
    }
}
