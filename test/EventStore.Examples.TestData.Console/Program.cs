namespace EventStore.Examples.TestData.Console
{
    using System;
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using ClientAPI;
    using ClientAPI.Common.Log;
    using Domain;
    using EventStore.Examples.Helpers.Configuration;
    using Helpers.Serialization;

    class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Adding account test data");
            
            var connection = EventStoreConnectionFactory.Create(
                "ConnectTo=tcp://localhost:1113",
                "admin", "changeit", new ConsoleLogger());

            await connection.ConnectAsync();
            
            var tasks = new Task[4];

            for (int i = 0; i < 4; i++)
            {
                var threadNumber = i;
                tasks[i] = Task.Run(async () =>
                {
                    var stream = $"Account-{threadNumber}";
                    var createdEvent = new AccountCreated()
                    {
                        AccountNumber = $"40{threadNumber}"
                    };

                    var createdEventData = new EventData(Guid.NewGuid(), "AccountCreated", true,  EventJsonSerializer.SerializeObject(createdEvent), null);

                    await connection.ConditionalAppendToStreamAsync(stream, ExpectedVersion.EmptyStream, new[] {createdEventData});

                    for (int j = 0; j < 100; j++)
                    {
                        var transactions = new List<EventData>();
                        for (int k = 0; k < 500; k++)
                        {
                            transactions.Add(new EventData(
                                Guid.NewGuid(), 
                                "AccountDebited", 
                                true,
                                EventJsonSerializer.SerializeObject(new AccountDebited {Amount = new Random().Next(1, 100)}), null));
                        
                            transactions.Add(new EventData(
                                Guid.NewGuid(), 
                                "AccountCredited", 
                                true,
                                EventJsonSerializer.SerializeObject(new AccountCredited {Amount = new Random().Next(1, 100)}), null));
                        }

                        var result = await connection.ConditionalAppendToStreamAsync(stream, j * 1000, transactions);
                        Console.WriteLine($"Added events for {threadNumber}, version: {j * 1000}, result: {result.Status}");
                    }
                });
            }

            Task.WaitAll(tasks);
            
            Console.WriteLine("Test data added.");
            Console.ReadLine();
        }
    }
}