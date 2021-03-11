using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory() { HostName = "localhost" };

using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    channel.QueueDeclare(
        queue: "task-queue",
        durable: true, // Garante que a fila irá sobreviver ao reinício do node RabbitMQ
        exclusive: false,
        autoDelete: false,
        arguments: null);

    channel.BasicQos(0, prefetchCount: 1, false); 
    // Quando o parâmetro prefetchCount = 1, diz que o Worker não deve receber mais de uma tarefa ao mesmo tempo
    // Evita que um Worker esteja mais ocupado que o outro, recebendo as tarefas pesadas e o outro as leves

    var consumer = new EventingBasicConsumer(channel);

    consumer.Received += 
        new EventHandler<BasicDeliverEventArgs>((sender, ea) => EventReceive(sender, ea, channel));

    channel.BasicConsume(
        queue: "task-queue",
        autoAck: false, // False = Necessário definir manualmente a confirmação de término da tarefa
        consumer: consumer);

    Console.WriteLine(" Press [enter] to exit.");
    Console.ReadLine();
}

static void EventReceive(object sender, BasicDeliverEventArgs ea, IModel channel) 
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");

    int dots = message.Split('.').Length - 1;
    Thread.Sleep(dots * 1000);

    Console.WriteLine(" [x] Done");

    // Envia uma confirmação manual a partir do Worker de término da tarefa
    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
}
