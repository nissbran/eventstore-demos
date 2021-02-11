using System.Net;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using EventStore.Examples.AdvancedRead.Console.Events;
using EventStore.Examples.Infrastructure.Logging;
using EventStore.Examples.Infrastructure.Serialization;
using Serilog;

namespace EventStore.Examples.AdvancedRead.Console
{
    using System;
    
    internal static class Program
    {
        private const string Username = "admin";
        private const string Password = "changeit";

        private const string StreamPrefix1 = "AdvancedReadStream";
        private const string StreamPrefix2 = "AdvancedReadStream2";

        private static IEventStoreConnection _eventStoreConnection;

        private static long _allAdvancedReadStreamCounter = 0;
        private static long _allResolvedAdvancedReadStreamValue = 0;
        private static long _allResolvedReadEvent2Value = 0;

        private static long _numberOfStreamsCounter = 0;
        private static long _numberOfAdvancedReadStream2Streams = 0;

        private static string _latestAdvancedReadStream2Stream = "";

        public static async Task Main(string[] args)
        {
            SerilogConsoleConfiguration.Configure();
            
            Log.Information("For this example to work you need to turn on all system projections in EventStore.");

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

            Log.Information("ReadAllAdvancedReadStreamEvents, Number of events: {NumberOfEvents}", _allAdvancedReadStreamCounter);
            Log.Information("ReadAllResolvedAdvancedReadStreamEvents, Total value: {NumberOfEvents}", _allResolvedAdvancedReadStreamValue);
            Log.Information("ReadAllResolvedReadEvent2Events, Total value: {NumberOfEvents}", _allResolvedReadEvent2Value);
            Log.Information("ReadAndCountAllStreams, Number of streams: {NumberOfEvents}", _numberOfStreamsCounter);
            Log.Information("ReadAndCountAllAdvancedReadStream2Streams, Number of streams: {NumberOfEvents}", _numberOfAdvancedReadStream2Streams);
            Log.Information("Latest {StreamPrefix2} stream: {LatestStream}", StreamPrefix2, _latestAdvancedReadStream2Stream);

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
                    stream: "$ce-AdvancedReadStream",
                    start: nextSliceStart,
                    count: ReadBatchSize,
                    resolveLinkTos: false);

                nextSliceStart = currentSlice.NextEventNumber;

                _allAdvancedReadStreamCounter += currentSlice.Events.Length;

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
                    stream: "$ce-AdvancedReadStream",
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
                            _allResolvedAdvancedReadStreamValue += data2.Value;
                            break;

                        case "ReadEvent":
                            var data = EventJsonDeserializer.DeserializeObject<ReadEvent>(eventData.Event.Data);
                            _allResolvedAdvancedReadStreamValue += data.Value;
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
                    _allResolvedReadEvent2Value += data2.Value;
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

                _numberOfStreamsCounter += currentSlice.Events.Length;

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

                _numberOfAdvancedReadStream2Streams += currentSlice.Events.Length;

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

                _latestAdvancedReadStream2Stream = Encoding.UTF8.GetString(eventData);
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

                var result = await _eventStoreConnection.ConditionalAppendToStreamAsync($"{StreamPrefix1}-{i + 1}", ExpectedVersion.NoStream, events);
            }

            for (int i = 0; i < 100; i++)
            {
                var events = new EventData[50];

                for (int y = 0; y < 50; y++)
                {
                    events[y] = CreateEvent("ReadEvent2");
                }

                var result = await _eventStoreConnection.ConditionalAppendToStreamAsync($"{StreamPrefix2}-{i + 1}", ExpectedVersion.NoStream, events);
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
