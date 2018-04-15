namespace EventStore.Examples.Subscriptions.Console
{
    using EventStore.ClientAPI;
    using EventStore.ClientAPI.SystemData;
    using System;
    using System.Net;
    using System.Threading.Tasks;

    class Program
    {
        private const string Username = "admin";
        private const string Password = "changeit";

        private const string Stream = "SubscriptionStream-1";

        private static IEventStoreConnection _eventStoreConnection;

        private static long SubscribeToAllFromCounter = 0;
        private static long SubscribeToAllAsyncCounter = 0;
        private static long SubscribeToStreamFromCounter = 0;
        private static long SubscribeToStreamAsyncCounter = 0;

        public static async Task Main(string[] args)
        {
            var connectionSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials(Username, Password));

            _eventStoreConnection = EventStoreConnection.Create(connectionSettings, new IPEndPoint(IPAddress.Loopback, 1113));

            await _eventStoreConnection.ConnectAsync();

            await CreateStreamData();


            Start_SubscribeToAllFrom_Subscription();

            await Start_SubscribeToAllAsync_Subscription();

            Start_SubscribeToStreamFrom_Subscription();

            await Start_SubscribeToStreamAsync_Subscription();

            Console.ReadLine();

            Console.WriteLine($"SubscribeToAllFrom, Number of events processed {SubscribeToAllFromCounter}");
            Console.WriteLine($"SubscribeToAllAsync, Number of events processed {SubscribeToAllAsyncCounter}");
            Console.WriteLine($"SubscribeToStreamFrom, Number of events processed {SubscribeToStreamFromCounter}");
            Console.WriteLine($"SubscribeToStreamAsync, Number of events processed {SubscribeToStreamAsyncCounter}");

            Console.ReadLine();
        }

        private static void Start_SubscribeToAllFrom_Subscription()
        {
            Console.WriteLine(
                "Start SubscribeToAllFrom subscription. This subscriptions starts from a given point and gets all events from that position." +
                " When the subscription have 'caught up' then it switches to a live subscription.");

            var subscription = _eventStoreConnection.SubscribeToAllFrom(
                lastCheckpoint: Position.Start,
                settings: new CatchUpSubscriptionSettings(10000, 500, false, false, "CatchAllSubscription"), 
                eventAppeared: SubscribeToAllFromEventAppeared);

            Console.WriteLine($"Subscription started: {subscription.SubscriptionName}");
        }

        private static Task SubscribeToAllFromEventAppeared(EventStoreCatchUpSubscription subscription, ResolvedEvent resolvedEvent)
        {
            //Console.WriteLine(resolvedEvent.Event.EventType);

            SubscribeToAllFromCounter++;

            return Task.CompletedTask;
        }

        private static async Task Start_SubscribeToAllAsync_Subscription()
        {
            Console.WriteLine("Start SubscribeToAllAsync subscription. This subscription only process live data. It has no catchup function.");

            var subscription = await _eventStoreConnection.SubscribeToAllAsync(
                resolveLinkTos: false, 
                eventAppeared: SubscribeToAllAsyncEventAppeared);

            Console.WriteLine($"SubscribeToAllAsync started");
        }

        private static Task SubscribeToAllAsyncEventAppeared(EventStoreSubscription subscription, ResolvedEvent resolvedEvent)
        {
            //Console.WriteLine(resolvedEvent.Event.EventType);

            SubscribeToAllAsyncCounter++;

            return Task.CompletedTask;
        }

        private static void Start_SubscribeToStreamFrom_Subscription()
        {
            Console.WriteLine(
                "Start SubscribeToStreamFrom subscription. This subscriptions starts from a given stream position and gets all stream events from that position." +
                " When the subscription have 'caught up' then it switches to a live subscription.");
            var subscription = _eventStoreConnection.SubscribeToStreamFrom(
                stream: Stream,
                lastCheckpoint: StreamPosition.Start,
                settings: new CatchUpSubscriptionSettings(10000, 500, false, false, "SubscribeToStreamFromSubscription"),
                eventAppeared: SubscribeToStreamFromEventAppeared);

            Console.WriteLine($"SubscribeToStreamFrom started");
        }

        private static Task SubscribeToStreamFromEventAppeared(EventStoreCatchUpSubscription subscription, ResolvedEvent resolvedEvent)
        {
            //Console.WriteLine(resolvedEvent.Event.EventType);

            SubscribeToStreamFromCounter++;

            return Task.CompletedTask;
        }

        private static async Task Start_SubscribeToStreamAsync_Subscription()
        {
            Console.WriteLine("Start SubscribeToStreamAsync subscription. This subscription only process live data. It has no catchup function.");

            var subscription = await _eventStoreConnection.SubscribeToStreamAsync(
                stream: Stream,
                resolveLinkTos: false,
                eventAppeared: SubscribeToStreamAsyncEventAppeared);

            Console.WriteLine($"SubscribeToAllAsync started");
        }

        private static Task SubscribeToStreamAsyncEventAppeared(EventStoreSubscription subscription, ResolvedEvent resolvedEvent)
        {
            //Console.WriteLine(resolvedEvent.Event.EventType);

            SubscribeToStreamAsyncCounter++;

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

        private static EventData CreateEvent(int number)
        {
            return new EventData(Guid.NewGuid(), $"SubscriptionEvent-{number}", false, null, null);
        }
    }
}
