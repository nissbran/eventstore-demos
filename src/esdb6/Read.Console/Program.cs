using System.Linq;
using System.Threading.Tasks;
using EventStore.Client;
using EventStore.Examples.Infrastructure.ESdb6;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace EventStore.Examples.Read.ESdb6.Console
{
    internal static class Program
    {
        private const string StreamName = "ReadStream-1";

        private static EventStoreClient _eventStoreClient;

        public static async Task Main(string[] args)
        {
            ConfigureLogger();
            
            _eventStoreClient = EventStoreConnectionFactory.Create("esdb://127.0.0.1:2113");

            await CreateStreamData();

            await ReadAllEventsForward();
            await ReadAllEventsBackwards();
            await ReadStreamForward();
            await ReadStreamBackwards();
            await ReadSingleEvent();

            System.Console.ReadLine();
        }

        private static async Task ReadAllEventsForward()
        {
            Log.Information("Reading all events from EventStore start and 1000 events Forwards with ReadStreamAsync");

            var readSlice = await _eventStoreClient.ReadAllAsync(Direction.Forwards, Position.Start, 1000, resolveLinkTos: false).ToListAsync();
            
            Log.Information("Number of events: {NumberOfEvents} with read direction: {ReadDirection}, NextPositionToRead: {Position}", 
                readSlice.Count, Direction.Forwards, readSlice.Last().OriginalPosition);
        }

        private static async Task ReadAllEventsBackwards()
        {
            Log.Information("Reading the 1000 latest/newest events from EventStore with ReadAllEventsBackwardAsync");

            var readSlice = await _eventStoreClient.ReadAllAsync(Direction.Backwards, Position.End, 1000).ToListAsync();
            
            Log.Information("Number of events: {NumberOfEvents} with read direction: {ReadDirection}, NextPositionToRead: {Position}", 
                readSlice.Count, Direction.Backwards, readSlice.Last().OriginalPosition);
        }

        private static async Task ReadStreamForward()
        {
            Log.Information("Reading 5 events from stream: {StreamName} with ReadStreamEventsForwardAsync", StreamName);

            var readSlice = await _eventStoreClient.ReadStreamAsync(Direction.Forwards, StreamName, StreamPosition.Start, 5).ToListAsync();

            Log.Information("Number of events: {NumberOfEvents} with read direction: {ReadDirection}, NextEventToRead: {NextEventNumber}", 
                readSlice.Count, Direction.Forwards, 5);

            foreach (var eventData in readSlice)
            {
                Log.Information($"Event: {eventData.Event.EventType}");
            }
        }

        private static async Task ReadStreamBackwards()
        {
            Log.Information($"Reading 5 events from stream: {StreamName} with ReadStreamEventsBackwardAsync");

            var readSlice = await _eventStoreClient.ReadStreamAsync(Direction.Backwards, StreamName, StreamPosition.End, 5).ToListAsync();

            Log.Information("Number of events: {NumberOfEvents} with read direction: {ReadDirection}, NextEventToRead: {NextEventNumber}", 
                readSlice.Count, Direction.Backwards, 5);

            foreach (var eventData in readSlice)
            {
                Log.Information($"Event: {eventData.Event.EventType}");
            }
        }

        private static async Task ReadSingleEvent()
        {
            Log.Information("Reading event number 3 from stream: {StreamName} with ReadEventAsync", StreamName);

            var eventReadResult = await _eventStoreClient.ReadStreamAsync(Direction.Forwards, StreamName, 3, 1).ToListAsync();

            Log.Information("Count: {Count}, Event type: {Type} ", eventReadResult.Count, eventReadResult.First().Event.EventType);
        }

        private static async Task CreateStreamData()
        {
            var result = await _eventStoreClient.ConditionalAppendToStreamAsync(StreamName, StreamState.NoStream, new[]
            {
                CreateEvent(1),
                CreateEvent(2),
                CreateEvent(3),
                CreateEvent(4),
                CreateEvent(5)
            });
        }

        private static EventData CreateEvent(int number)
        {
            return new EventData(Uuid.NewUuid(), $"ReadEvent-{number}", null);
        }
        
        private static void ConfigureLogger()
        {
            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.Console(theme: AnsiConsoleTheme.Code)
                .CreateLogger();
        }
    }
}
