using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Common.Log;
using EventStore.Examples.Domain;
using EventStore.Examples.Infrastructure.ESdb5.Configuration;
using EventStore.Examples.Infrastructure.Logging;
using EventStore.Examples.Infrastructure.Serialization;
using Serilog;

namespace EventStore.Examples.TestData.ESdb5.Console
{
    class Program
    {
        private const int NumberOfThreads = 10;
        private static long _numberOfEvents = 0; 
        
        public static async Task Main(string[] args)
        {
            SerilogConsoleConfiguration.Configure();
            
            Log.Information("Adding account test data");
            
            var connection = EventStoreConnectionFactory.Create(
                // "GossipSeeds=35.228.147.42:2113,35.228.245.228:2113,35.228.119.72:2113",
                //"ConnectTo=tcp://127.0.0.1:1113",
                "GossipSeeds=127.0.0.1:2113,127.0.0.1:2123,127.0.0.1:2133",
                "admin", "changeit"
                ,new ConsoleLogger()
            );

            await connection.ConnectAsync();

            InsertTestData1(connection);
            await VerifyTestData1(connection);
            
            Log.Information("Test data batch 1 inserted. Click any key to continue to next batch...");
            System.Console.ReadLine();
            
            InsertTestData2(connection);
            await VerifyTestData2(connection);
            System.Console.ReadLine();
        }

        private static void InsertTestData1(IEventStoreConnection connection)
        {
            var tasks = new Task[NumberOfThreads];

            for (int i = 0; i < NumberOfThreads; i++)
            {
                var threadNumber = i;
                tasks[i] = Task.Run(async () =>
                {
                    var stream = CreateStreamName1(threadNumber);
                    var createdEvent = new AccountCreated
                    {
                        AccountNumber = $"40{threadNumber}"
                    };

                    var createdEventData = new EventData(Guid.NewGuid(), "AccountCreated", true, EventJsonSerializer.SerializeObject(createdEvent), null);

                    await connection.ConditionalAppendToStreamAsync(stream, ExpectedVersion.NoStream, new[] {createdEventData});

                    for (int j = 0; j < 100; j++)
                    {
                        var transactions = new List<EventData>();
                        for (int k = 0; k < 50; k++)
                        {
                            transactions.Add(new EventData(
                                Guid.NewGuid(),
                                "AccountDebited",
                                true,
                                EventJsonSerializer.SerializeObject(new AccountDebited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 40{threadNumber} which is debited",
                                    Description2 = $"A nice description of the current event for account 40{threadNumber} which is debited",
                                    Description3 = $"A nice description of the current event for account 40{threadNumber} which is debited"
                                }), null));

                            transactions.Add(new EventData(
                                Guid.NewGuid(),
                                "AccountCredited",
                                true,
                                EventJsonSerializer.SerializeObject(new AccountCredited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 40{threadNumber} which is credited",
                                    Description2 = $"A nice description of the current event for account 40{threadNumber} which is credited",
                                    Description3 = $"A nice description of the current event for account 40{threadNumber} which is credited"
                                }), null));
                        }

                        var expectedVersion = (long) (j * 100);
                        var result = await connection.ConditionalAppendToStreamAsync(stream, expectedVersion, transactions);
                        Log.Information("Added events for {ThreadNumber}, version: {Version}, result: {Status}", threadNumber, j * 100, result.Status);
                    }
                });
            }

            Task.WaitAll(tasks);
        }

        private static async Task VerifyTestData1(IEventStoreConnection connection)
        {
            var totalNumberOfEvents = 0;
            for (int i = 0; i < NumberOfThreads; i++)
            {
                var streamName = CreateStreamName1(i);
                var totalEventsInStream = 0;
                var numberOfAccountDebitedEventsInStream = 0;
                var numberOfAccountCreditedEventsInStream = 0;

                StreamEventsSlice currentSlice;
                long nextSliceStart = StreamPosition.Start;
                do
                {
                    currentSlice = await connection.ReadStreamEventsForwardAsync(streamName, nextSliceStart, 1000, false);
                    nextSliceStart = currentSlice.NextEventNumber;
                    foreach (var resolvedEvent in currentSlice.Events)
                    {
                        totalNumberOfEvents++;
                        totalEventsInStream++;
                        if (resolvedEvent.Event.EventType == "AccountDebited")
                            numberOfAccountDebitedEventsInStream++;
                        if (resolvedEvent.Event.EventType == "AccountCredited")
                            numberOfAccountCreditedEventsInStream++;
                    }
                } while (!currentSlice.IsEndOfStream);

                Log.Information("Number of events for stream '{StreamName}', total: {Total}, debited {Debited}, credited {Credited}",
                    streamName, totalEventsInStream, numberOfAccountDebitedEventsInStream, numberOfAccountCreditedEventsInStream);
            }

            Log.Information("Test data added. Total number of events: {Events}", totalNumberOfEvents);
        }

        private static string CreateStreamName1(int threadNumber) => $"Account-{threadNumber}";
        
        private static void InsertTestData2(IEventStoreConnection connection)
        {
            var tasks = new Task[NumberOfThreads];

            for (int i = 0; i < NumberOfThreads; i++)
            {
                var threadNumber = i;
                tasks[i] = Task.Run(async () =>
                {
                    var stream = CreateStreamName2(threadNumber);
                    var createdEvent = new AccountCreated
                    {
                        AccountNumber = $"60{threadNumber}"
                    };

                    var createdEventData = new EventData(Guid.NewGuid(), "AccountCreated", true, EventJsonSerializer.SerializeObject(createdEvent), null);

                    await connection.ConditionalAppendToStreamAsync(stream, ExpectedVersion.NoStream, new[] {createdEventData});

                    for (int j = 0; j < 100; j++)
                    {
                        var transactions = new List<EventData>();
                        for (int k = 0; k < 500; k++)
                        {
                            transactions.Add(new EventData(
                                Guid.NewGuid(),
                                "AccountDebited",
                                true,
                                EventJsonSerializer.SerializeObject(new AccountDebited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 60{threadNumber} which is debited",
                                    Description2 = $"A nice description of the current event for account 60{threadNumber} which is debited",
                                    Description3 = $"A nice description of the current event for account 60{threadNumber} which is debited"
                                }), null));

                            transactions.Add(new EventData(
                                Guid.NewGuid(),
                                "AccountCredited",
                                true,
                                EventJsonSerializer.SerializeObject(new AccountCredited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 60{threadNumber} which is credited",
                                    Description2 = $"A nice description of the current event for account 60{threadNumber} which is credited",
                                    Description3 = $"A nice description of the current event for account 60{threadNumber} which is credited"
                                }), null));
                        }

                        var expectedVersion = (long) (j * 1000);
                        var result = await connection.ConditionalAppendToStreamAsync(stream, expectedVersion, transactions);
                        Log.Information("Added events for {Stream}, version: {Version}, result: {Status}", stream, j * 1000, result.Status);
                    }
                });
            }

            Task.WaitAll(tasks);
        }
        
        private static async Task VerifyTestData2(IEventStoreConnection connection)
        {
            var totalNumberOfEvents = 0;
            for (int i = 0; i < NumberOfThreads; i++)
            {
                var streamName = CreateStreamName2(i);
                var totalEventsInStream = 0;
                var numberOfAccountDebitedEventsInStream = 0;
                var numberOfAccountCreditedEventsInStream = 0;

                StreamEventsSlice currentSlice;
                long nextSliceStart = StreamPosition.Start;
                do
                {
                    currentSlice = await connection.ReadStreamEventsForwardAsync(streamName, nextSliceStart, 1000, false);
                    nextSliceStart = currentSlice.NextEventNumber;
                    foreach (var resolvedEvent in currentSlice.Events)
                    {
                        totalNumberOfEvents++;
                        totalEventsInStream++;
                        if (resolvedEvent.Event.EventType == "AccountDebited")
                            numberOfAccountDebitedEventsInStream++;
                        if (resolvedEvent.Event.EventType == "AccountCredited")
                            numberOfAccountCreditedEventsInStream++;
                    }
                } while (!currentSlice.IsEndOfStream);

                Log.Information("Number of events for stream '{StreamName}', total: {Total}, debited {Debited}, credited {Credited}",
                    streamName, totalEventsInStream, numberOfAccountDebitedEventsInStream, numberOfAccountCreditedEventsInStream);
            }

            Log.Information("Test data added. Total number of events: {Events}", totalNumberOfEvents);
        }
        
        private static string CreateStreamName2(int threadNumber) => $"Account-X{threadNumber}";

    }
}