using System.Text;
using System.Text.Json.Serialization.Metadata;
using RabbitMQ.Client;

var queueName = "A";
var exit = false;
var factory = new ConnectionFactory { HostName = "localhost" };
var count = 0;

Console.CancelKeyPress += (sender, eventArgs) =>
{
    if (eventArgs.SpecialKey == ConsoleSpecialKey.ControlC)
        exit = true;

    eventArgs.Cancel = true;
};

using (var connection = factory.CreateConnection())
using (var channel = connection.CreateModel())
{
    //Durable говорит о том, что очередь будет оставаться после перезагрузки, если есть сообщения
    var declarationResult = channel.QueueDeclare(queueName, true, false, false);
    var basicProperties = channel.CreateBasicProperties();
    basicProperties.Persistent = true; //Говорит о том, что сообщения будут сохранятся на диск

    if (declarationResult == null)
    {
        Console.WriteLine("Cannot declare or connect to queue ");
        return;
    }

    while (!exit)
    {
        Thread.Sleep(Random.Shared.Next(1000, 4000));
        
        try
        {
            var message = $"Message #{count++}";
            var body = Encoding.UTF8.GetBytes(message);
            
            channel.BasicPublish(exchange: string.Empty,
                                 routingKey: queueName,
                                 basicProperties: basicProperties,
                                 body: body);
            
            Console.WriteLine($"Message sent: {message}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }
}

Console.WriteLine("Press any button to exit...");
Console.Read();