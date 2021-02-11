using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EventStore.Client;
using EventStore.Examples.Domain;
using EventStore.Examples.Infrastructure.ESdb6;
using EventStore.Examples.Infrastructure.Logging;
using EventStore.Examples.Infrastructure.Serialization;
using Grpc.Core;
using Serilog;

namespace EventStore.Examples.TestData.ESdb6.Console
{
    class Program
    {
        private const int NumberOfThreads = 10;
        private static EventStoreClient _client;
        
        public static async Task Main(string[] args)
        {
            SerilogConsoleConfiguration.Configure();
            
            Log.Information("Adding account test data");

            _client = EventStoreConnectionFactory.Create();

            InsertTestData1();
            await VerifyTestData1();
            
            Log.Information("Test data batch 1 inserted. Click any key to continue to next batch...");
            System.Console.ReadLine();
            
            InsertTestData2();
            await VerifyTestData2();
            System.Console.ReadLine();
        }

        private static void InsertTestData1()
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

                    var createdEventData = new EventData(Uuid.NewUuid(), "AccountCreated", EventJsonSerializer.SerializeObject(createdEvent));

                    await _client.ConditionalAppendToStreamAsync(stream, StreamState.NoStream, new[] {createdEventData});

                    for (int j = 0; j < 100; j++)
                    {
                        var transactions = new List<EventData>();
                        for (int k = 0; k < 50; k++)
                        {
                            transactions.Add(new EventData(
                                Uuid.NewUuid(),
                                "AccountDebited",
                                EventJsonSerializer.SerializeObject(new AccountDebited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 40{threadNumber} which is debited",
                                    Description2 = $"A nice description of the current event for account 40{threadNumber} which is debited",
                                    Description3 = $"A nice description of the current event for account 40{threadNumber} which is debited"
                                })));

                            transactions.Add(new EventData(
                                Uuid.NewUuid(),
                                "AccountCredited",
                                EventJsonSerializer.SerializeObject(new AccountCredited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 40{threadNumber} which is credited",
                                    Description2 = $"A nice description of the current event for account 40{threadNumber} which is credited",
                                    Description3 = $"A nice description of the current event for account 40{threadNumber} which is credited"
                                })));
                        }

                        var expectedVersion = StreamRevision.FromInt64(j * 100);
                        var result = await AppendWithRetry(stream, expectedVersion, transactions);
                        Log.Information("Added events for {ThreadNumber}, version: {Version}, result: {Status}", threadNumber, j * 100, result.Status);
                    }
                });
            }

            Task.WaitAll(tasks);
        }

        private static async Task VerifyTestData1()
        {
            var totalNumberOfEvents = 0;
            for (int i = 0; i < NumberOfThreads; i++)
            {
                var streamName = CreateStreamName1(i);
                var totalEventsInStream = 0;
                var numberOfAccountDebitedEventsInStream = 0;
                var numberOfAccountCreditedEventsInStream = 0;

                await ReadStreamForwardWithRetry(streamName, resolvedEvent =>
                {
                    totalNumberOfEvents++;
                    totalEventsInStream++;
                    if (resolvedEvent.Event.EventType == "AccountDebited")
                        numberOfAccountDebitedEventsInStream++;
                    if (resolvedEvent.Event.EventType == "AccountCredited")
                        numberOfAccountCreditedEventsInStream++;
                });

                Log.Information("Number of events for stream '{StreamName}', total: {Total}, debited {Debited}, credited {Credited}",
                    streamName, totalEventsInStream, numberOfAccountDebitedEventsInStream, numberOfAccountCreditedEventsInStream);
            }

            Log.Information("Test data added. Total number of events: {Events}", totalNumberOfEvents);
        }

        private static string CreateStreamName1(int threadNumber) => $"Account-{threadNumber}";
        
        private static void InsertTestData2()
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
        
                    var createdEventData = new EventData(Uuid.NewUuid(), "AccountCreated", EventJsonSerializer.SerializeObject(createdEvent));
        
                    await _client.ConditionalAppendToStreamAsync(stream, StreamState.NoStream, new[] {createdEventData});
        
                    for (int j = 0; j < 100; j++)
                    {
                        var transactions = new List<EventData>();
                        for (int k = 0; k < 500; k++)
                        {
                            transactions.Add(new EventData(
                                Uuid.NewUuid(),
                                "AccountDebited",
                                EventJsonSerializer.SerializeObject(new AccountDebited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 60{threadNumber} which is debited",
                                    Description2 = $"A nice description of the current event for account 60{threadNumber} which is debited",
                                    Description3 = $"A nice description of the current event for account 60{threadNumber} which is debited"
                                }), null));
        
                            transactions.Add(new EventData(
                                Uuid.NewUuid(),
                                "AccountCredited",
                                EventJsonSerializer.SerializeObject(new AccountCredited
                                {
                                    Amount = new Random().Next(1, 100),
                                    Description = $"A nice description of the current event for account 60{threadNumber} which is credited",
                                    Description2 = $"A nice description of the current event for account 60{threadNumber} which is credited",
                                    Description3 = $"A nice description of the current event for account 60{threadNumber} which is credited"
                                })));
                        }
        
                        var expectedVersion = StreamRevision.FromInt64(j * 1000);
                        var result = await AppendWithRetry(stream, expectedVersion, transactions);
                        Log.Information("Added events for {Stream}, version: {Version}, result: {Status}", stream, j * 1000, result.Status);
                    }
                });
            }
        
            Task.WaitAll(tasks);
        }
        
        private static async Task VerifyTestData2()
        {
            var totalNumberOfEvents = 0;
            for (int i = 0; i < NumberOfThreads; i++)
            {
                var streamName = CreateStreamName2(i);
                var totalEventsInStream = 0;
                var numberOfAccountDebitedEventsInStream = 0;
                var numberOfAccountCreditedEventsInStream = 0;

                await ReadStreamForwardWithRetry(streamName, resolvedEvent =>
                {
                    if (totalEventsInStream != 0 && totalEventsInStream % 10000 == 0)
                        Log.Information("Number of events read in {Stream}: {NumberOfEvents}", streamName, totalEventsInStream);

                    totalNumberOfEvents++;
                    totalEventsInStream++;
                    if (resolvedEvent.Event.EventType == "AccountDebited")
                        numberOfAccountDebitedEventsInStream++;
                    if (resolvedEvent.Event.EventType == "AccountCredited")
                        numberOfAccountCreditedEventsInStream++;
                });
        
                Log.Information("Number of events for stream '{StreamName}', total: {Total}, debited {Debited}, credited {Credited}",
                    streamName, totalEventsInStream, numberOfAccountDebitedEventsInStream, numberOfAccountCreditedEventsInStream);
            }
        
            Log.Information("Test data added. Total number of events: {Events}", totalNumberOfEvents);
        }
        
        private static string CreateStreamName2(int threadNumber) => $"Account-X{threadNumber}";

        private static async Task<ConditionalWriteResult> AppendWithRetry(string stream, StreamRevision expectedStreamRevision, List<EventData> events)
        {
            return await ExecuteWithRetryAsync(async () => await _client.ConditionalAppendToStreamAsync(stream, expectedStreamRevision, events),
                exception => exception is NotLeaderException || exception is RpcException || exception is InvalidOperationException, 
                (exception, i) => Log.Warning(exception, $"Warning, append failed. Retrying in 500 ms, retry count {i}"), TimeSpan.FromMilliseconds(500), 5);
        }

        private static async Task ReadStreamForwardWithRetry(string stream, Action<ResolvedEvent> eventAppeared)
        {
            var currentStreamPosition = StreamPosition.Start;
            
            await ExecuteWithRetryAsync(async () =>
                {
                    var readStream = _client.ReadStreamAsync(Direction.Forwards, stream, currentStreamPosition);
                    
                    await foreach (var resolvedEvent in readStream)
                    {
                        eventAppeared(resolvedEvent);
                        currentStreamPosition++;
                    }

                    return currentStreamPosition;
                },
                exception => exception is NotLeaderException || exception is RpcException || exception is InvalidOperationException,
                (exception, i) => Log.Warning(exception, $"Warning, read failed. Retrying in 500 ms, retry count {i}"), TimeSpan.FromMilliseconds(500), 10);
        }
        
        private static async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> func, Func<Exception, bool> shouldRetry, Action<Exception, int> traceRetry, TimeSpan delay, int times = 3)
        {
            var initialTimes = times;
            while (true)
            {
                times--;
                try
                {
                    return await func();
                }
                catch (Exception ex)
                {
                    if (!shouldRetry(ex))
                        throw;
                    if (times <= 0) 
                        throw;
                    await Task.Delay(delay);
                    delay += delay;
                    traceRetry(ex, initialTimes - times);
                }
            }
        }
    }
}