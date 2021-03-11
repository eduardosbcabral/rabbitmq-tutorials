using System;
using RabbitMQ.Client;
using System.Text;

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

    var message = "Hello World using RabbitMQ!";

    var body = Encoding.UTF8.GetBytes(message);

    channel.BasicPublish(
        exchange: "",
        routingKey: "hello-queue",
        basicProperties: null,
        body: body);

    Console.WriteLine($" [x] Sent {message}");
}

Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();