namespace EventStore.Examples.AdvancedRead.Console
{
    using EventStore.ClientAPI;
    using EventStore.ClientAPI.SystemData;
    using EventStore.Examples.AdvancedRead.Console.Events;
    using EventStore.Examples.Helpers.Serialization;
    using System;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;

    class Program
    {
        private const string Username = "admin";
        private const string Password = "changeit";

        private const string StreamPrefix1 = "AdvancedReadStream";
        private const string StreamPrefix2 = "AdvancedReadStream2";

        private static IEventStoreConnection _eventStoreConnection;

        private static long AllAdvancedReadStreamCounter = 0;
        private static long AllResolvedAdvancedReadStreamValue = 0;
        private static long AllResolvedReadEvent2Value = 0;

        private static long NumberOfStreamsCounter = 0;
        private static long NumberOfAdvancedReadStream2Streams = 0;

        private static string LatestAdvancedReadStream2Stream = "";

        public static async Task Main(string[] args)
        {
            Console.WriteLine("For this example to work you need to turn on all system projections in EventStore.");

            var connectionSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials(Username, Password));

            _eventStoreConnection = EventStoreConnection.Create(connectionSettings, new IPEndPoint(IPAddress.Loopback, 1113));

            await _eventStoreConnection.ConnectAsync();

            await CreateStreamData();

            await ReadAllAdvancedReadStreamEvents();
            await ReadAllResolvedAdvancedReadStreamEvents();
            await ReadAllResolvedReadEvent2Events();
            await ReadAndCountAllStreams();
            await ReadAndCountAllAdvancedReadStream2Streams();
            await ReadTheNewestAdvancedReadStream2Stream();

            Console.WriteLine($"ReadAllAdvancedReadStreamEvents, Number of events: {AllAdvancedReadStreamCounter}");
            Console.WriteLine($"ReadAllResolvedAdvancedReadStreamEvents, Total value: {AllResolvedAdvancedReadStreamValue}");
            Console.WriteLine($"ReadAllResolvedReadEvent2Events, Total value: {AllResolvedReadEvent2Value}");
            Console.WriteLine($"ReadAndCountAllStreams, Number of streams: {NumberOfStreamsCounter}");
            Console.WriteLine($"ReadAndCountAllAdvancedReadStream2Streams, Number of streams: {NumberOfAdvancedReadStream2Streams}");
            Console.WriteLine($"Latest {StreamPrefix2} stream: {LatestAdvancedReadStream2Stream}");

            Console.ReadLine();
        }

        private static async Task ReadAllAdvancedReadStreamEvents()
        {
            const int ReadBatchSize = 200;
            StreamEventsSlice currentSlice;
            var nextSliceStart = (long)StreamPosition.Start;
            do
            {
                currentSlice = await _eventStoreConnection.ReadStreamEventsForwardAsync(
                    stream: $"$ce-{StreamPrefix1}",
                    start: nextSliceStart,
                    count: ReadBatchSize,
                    resolveLinkTos: false);

                nextSliceStart = currentSlice.NextEventNumber;

                AllAdvancedReadStreamCounter += currentSlice.Events.Length;

            } while (!currentSlice.IsEndOfStream);
        }

        private static async Task ReadAllResolvedAdvancedReadStreamEvents()
        {
            const int ReadBatchSize = 250;
            StreamEventsSlice currentSlice;
            var nextSliceStart = (long)StreamPosition.Start;
            do
            {
                currentSlice = await _eventStoreConnection.ReadStreamEventsForwardAsync(
                    stream: $"$ce-{StreamPrefix1}",
                    start: nextSliceStart,
                    count: ReadBatchSize,
                    resolveLinkTos: true);

                nextSliceStart = currentSlice.NextEventNumber;

                foreach (var eventData in currentSlice.Events)
                {
                    switch (eventData.Event.EventType)
                    {
                        case "ReadEvent2":
                            var data2 = EventJsonDeserializer.DeserializeObject<ReadEvent2>(eventData.Event.Data);
                            AllResolvedAdvancedReadStreamValue += data2.Value;
                            break;

                        case "ReadEvent":
                            var data = EventJsonDeserializer.DeserializeObject<ReadEvent>(eventData.Event.Data);
                            AllResolvedAdvancedReadStreamValue += data.Value;
                            break;
                    }
                }

            } while (!currentSlice.IsEndOfStream);
        }

        private static async Task ReadAllResolvedReadEvent2Events()
        {
            const int ReadBatchSize = 250;
            StreamEventsSlice currentSlice;
            var nextSliceStart = (long)StreamPosition.Start;
            do
            {
                currentSlice = await _eventStoreConnection.ReadStreamEventsForwardAsync(
                    stream: "$et-ReadEvent2",
                    start: nextSliceStart,
                    count: ReadBatchSize,
                    resolveLinkTos: true);

                nextSliceStart = currentSlice.NextEventNumber;

                foreach (var eventData in currentSlice.Events)
                {
                    var data2 = EventJsonDeserializer.DeserializeObject<ReadEvent2>(eventData.Event.Data);
                    AllResolvedReadEvent2Value += data2.Value;
                }

            } while (!currentSlice.IsEndOfStream);
        }

        private static async Task ReadAndCountAllStreams()
        {
            const int ReadBatchSize = 250;
            StreamEventsSlice currentSlice;
            var nextSliceStart = (long)StreamPosition.Start;
            do
            {
                currentSlice = await _eventStoreConnection.ReadStreamEventsForwardAsync(
                    stream: "$streams",
                    start: nextSliceStart,
                    count: ReadBatchSize,
                    resolveLinkTos: false);

                nextSliceStart = currentSlice.NextEventNumber;

                NumberOfStreamsCounter += currentSlice.Events.Length;

            } while (!currentSlice.IsEndOfStream);
        }

        private static async Task ReadAndCountAllAdvancedReadStream2Streams()
        {
            const int ReadBatchSize = 250;
            StreamEventsSlice currentSlice;
            var nextSliceStart = (long)StreamPosition.Start;
            do
            {
                currentSlice = await _eventStoreConnection.ReadStreamEventsForwardAsync(
                    stream: "$category-AdvancedReadStream2",
                    start: nextSliceStart,
                    count: ReadBatchSize,
                    resolveLinkTos: false);

                nextSliceStart = currentSlice.NextEventNumber;

                NumberOfAdvancedReadStream2Streams += currentSlice.Events.Length;

            } while (!currentSlice.IsEndOfStream);
        }

        private static async Task ReadTheNewestAdvancedReadStream2Stream()
        {
            StreamEventsSlice currentSlice = await _eventStoreConnection.ReadStreamEventsBackwardAsync(
                    stream: "$category-AdvancedReadStream2",
                    start: StreamPosition.End,
                    count: 1,
                    resolveLinkTos: false);

            if (currentSlice.Events.Length == 1)
            {
                var eventData = currentSlice.Events[0].Event.Data;

                LatestAdvancedReadStream2Stream = Encoding.UTF8.GetString(eventData);
            }
        }

        private static async Task CreateStreamData()
        {
            for (int i = 0; i < 5; i++)
            {
                var events = new EventData[600];
                for (int y = 0; y < 500; y++)
                {
                    events[y] = CreateEvent("ReadEvent");
                }

                for (int y = 500; y < 600; y++)
                {
                    events[y] = CreateEvent("ReadEvent2");
                }

                var result = await _eventStoreConnection.ConditionalAppendToStreamAsync($"{StreamPrefix1}-{i}", ExpectedVersion.NoStream, events);
            }

            for (int i = 0; i < 100; i++)
            {
                var events = new EventData[50];

                for (int y = 0; y < 50; y++)
                {
                    events[y] = CreateEvent("ReadEvent2");
                }

                var result = await _eventStoreConnection.ConditionalAppendToStreamAsync($"{StreamPrefix2}-{i}", ExpectedVersion.NoStream, events);
            }
        }

        private static EventData CreateEvent(string eventType)
        {
            switch (eventType)
            {
                case "ReadEvent2":
                    var eventJson2 = EventJsonSerializer.SerializeObject(new ReadEvent2 { Value = 2 });
                    return new EventData(Guid.NewGuid(), eventType, true, eventJson2, null);

                case "ReadEvent":
                default:
                    var eventJson = EventJsonSerializer.SerializeObject(new ReadEvent { Value = 1 });
                    return new EventData(Guid.NewGuid(), eventType, true, eventJson, null);
            }
        }
    }
}
