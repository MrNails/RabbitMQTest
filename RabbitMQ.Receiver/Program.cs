using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var queueName = "A";
var factory = new ConnectionFactory { HostName = "localhost" };
using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    //Durable говорит о том, что очередь будет оставаться после перезагрузки, если есть сообщения
    channel.QueueDeclare(queueName, true, false, false);
    
    //Говорит о том, что обработка сообщений будет идти по мере завершения обработки, а не по очереди 
    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += (model, ea) =>
    {
        var body = ea.Body.Span;
        var message = Encoding.UTF8.GetString(body);
        Console.WriteLine("Received {0}", message);
        
        Thread.Sleep(Random.Shared.Next(500, 1500));
        
        //Удаляем сообщение как только его обработали
        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
    };

    //Отключаем автоудаление (autoAck: false), чтобы избежать потери сообщения при отключении потребителя
    channel.BasicConsume(queue: queueName,
        autoAck: false,
        consumer: consumer);
    
    Console.WriteLine("Press any button to exit...");
    Console.Read();
}




