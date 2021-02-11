using System;
using System.Net;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Examples.Infrastructure.Logging;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace EventStore.Examples.Subscriptions.Console
{
    internal static class Program
    {
        private const string Username = "admin";
        private const string Password = "changeit";

        private const string Stream = "SubscriptionStream-1";

        private static IEventStoreConnection _eventStoreConnection;

        private static long _subscribeToAllFromCounter = 0;
        private static long _subscribeToAllAsyncCounter = 0;
        private static long _subscribeToStreamFromCounter = 0;
        private static long _subscribeToStreamAsyncCounter = 0;

        public static async Task Main(string[] args)
        {
            SerilogConsoleConfiguration.Configure();
            
            var connectionSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials(Username, Password));

            _eventStoreConnection = EventStoreConnection.Create(connectionSettings, new IPEndPoint(IPAddress.Loopback, 1113));

            await _eventStoreConnection.ConnectAsync();

            await CreateStreamData();


            Start_SubscribeToAllFrom_Subscription();

            await Start_SubscribeToAllAsync_Subscription();

            Start_SubscribeToStreamFrom_Subscription();

            await Start_SubscribeToStreamAsync_Subscription();

            System.Console.ReadLine();

            Log.Information("SubscribeToAllFrom, Number of events processed {NumberOfEvents}", _subscribeToAllFromCounter);
            Log.Information("SubscribeToAllAsync, Number of events processed {NumberOfEvents}", _subscribeToAllAsyncCounter);
            Log.Information("SubscribeToStreamFrom, Number of events processed {NumberOfEvents}", _subscribeToStreamFromCounter);
            Log.Information("SubscribeToStreamAsync, Number of events processed {NumberOfEvents}", _subscribeToStreamAsyncCounter);

            Log.Information("Creating 10 new events at {Stream}", Stream);
            
            await Create10NewEvents();
            
            System.Console.ReadLine();
            
            Log.Information("SubscribeToAllFrom, Number of events processed {NumberOfEvents}", _subscribeToAllFromCounter);
            Log.Information("SubscribeToAllAsync, Number of events processed {NumberOfEvents}", _subscribeToAllAsyncCounter);
            Log.Information("SubscribeToStreamFrom, Number of events processed {NumberOfEvents}", _subscribeToStreamFromCounter);
            Log.Information("SubscribeToStreamAsync, Number of events processed {NumberOfEvents}", _subscribeToStreamAsyncCounter);
            
            System.Console.ReadLine();
        }

        private static void Start_SubscribeToAllFrom_Subscription()
        {
            Log.Information(
                "Start SubscribeToAllFrom subscription. This subscriptions starts from a given point and gets all events from that position." +
                " When the subscription have 'caught up' then it switches to a live subscription.");

            var subscription = _eventStoreConnection.SubscribeToAllFrom(
                lastCheckpoint: Position.Start,
                settings: new CatchUpSubscriptionSettings(
                    maxLiveQueueSize: 10000, 
                    readBatchSize: 500,
                    verboseLogging: false, 
                    resolveLinkTos: false, 
                    subscriptionName: "CatchAllSubscription"), 
                eventAppeared: SubscribeToAllFromEventAppeared);

            Log.Information("Subscription started: {SubscriptionName}", subscription.SubscriptionName);
        }

        private static Task SubscribeToAllFromEventAppeared(EventStoreCatchUpSubscription subscription, ResolvedEvent resolvedEvent)
        {
            //Console.WriteLine(resolvedEvent.Event.EventType);

            _subscribeToAllFromCounter++;

            return Task.CompletedTask;
        }

        private static async Task Start_SubscribeToAllAsync_Subscription()
        {
            Log.Information("Start SubscribeToAllAsync subscription. This subscription only process live data. It has no catchup function.");

            var subscription = await _eventStoreConnection.SubscribeToAllAsync(
                resolveLinkTos: false, 
                eventAppeared: SubscribeToAllAsyncEventAppeared);

            Log.Information("SubscribeToAllAsync started");
        }

        private static Task SubscribeToAllAsyncEventAppeared(EventStoreSubscription subscription, ResolvedEvent resolvedEvent)
        {
            _subscribeToAllAsyncCounter++;

            return Task.CompletedTask;
        }

        private static void Start_SubscribeToStreamFrom_Subscription()
        {
            Log.Information(
                "Start SubscribeToStreamFrom subscription. This subscriptions starts from a given stream position and gets all stream events from that position." +
                " When the subscription have 'caught up' then it switches to a live subscription.");
            var subscription = _eventStoreConnection.SubscribeToStreamFrom(
                stream: Stream,
                lastCheckpoint: null,
                settings: new CatchUpSubscriptionSettings(10000, 500, false, false, "SubscribeToStreamFromSubscription"),
                eventAppeared: SubscribeToStreamFromEventAppeared);

            Log.Information("SubscribeToStreamFrom started");
        }

        private static Task SubscribeToStreamFromEventAppeared(EventStoreCatchUpSubscription subscription, ResolvedEvent resolvedEvent)
        {
            //Log.Information(resolvedEvent.Event.EventType);

            _subscribeToStreamFromCounter++;

            return Task.CompletedTask;
        }

        private static async Task Start_SubscribeToStreamAsync_Subscription()
        {
            Log.Information("Start SubscribeToStreamAsync subscription. This subscription only process live data. It has no catchup function.");

            var subscription = await _eventStoreConnection.SubscribeToStreamAsync(
                stream: Stream,
                resolveLinkTos: false,
                eventAppeared: SubscribeToStreamAsyncEventAppeared);

            Log.Information("SubscribeToAllAsync started");
        }

        private static Task SubscribeToStreamAsyncEventAppeared(EventStoreSubscription subscription, ResolvedEvent resolvedEvent)
        {
            _subscribeToStreamAsyncCounter++;

            return Task.CompletedTask;
        }

        private static async Task CreateStreamData()
        {
            var events = new EventData[500];
            for (int i = 0; i < 500; i++)
            {
                events[i] = CreateEvent(i);
            }

            var result = await _eventStoreConnection.ConditionalAppendToStreamAsync(Stream, ExpectedVersion.NoStream, events);
        }

        private static async Task Create10NewEvents()
        {
            var events = new EventData[10];
            for (int i = 0; i < 10; i++)
            {
                events[i] = CreateEvent(i + 500);
            }

            var result = await _eventStoreConnection.ConditionalAppendToStreamAsync(Stream, 499, events);
        }

        private static EventData CreateEvent(int number)
        {
            return new EventData(Guid.NewGuid(), $"SubscriptionEvent-{number}", false, null, null);
        }
    }
}
