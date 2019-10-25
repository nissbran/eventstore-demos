using System;
using System.Net;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;

namespace EventStore.Examples.Read.Console
{
    internal static class Program
    {
        private const string Username = "admin";
        private const string Password = "changeit";

        private const string Stream = "ReadStream-1";

        private static IEventStoreConnection _eventStoreConnection;

        public static async Task Main(string[] args)
        {
            var connectionSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials(Username, Password));

            _eventStoreConnection = EventStoreConnection.Create(connectionSettings, new IPEndPoint(IPAddress.Loopback, 1113));

            await _eventStoreConnection.ConnectAsync();

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
            System.Console.WriteLine("Reading all events from EventStore start and 1000 events forward with ReadAllEventsForwardAsync");

            var readSlice = await _eventStoreConnection.ReadAllEventsForwardAsync(Position.Start, 1000, false);
            
            System.Console.WriteLine($"Number of events: {readSlice.Events.Length} with read direction: {readSlice.ReadDirection}, NextPositionToRead: {readSlice.NextPosition}");
        }

        private static async Task ReadAllEventsBackwards()
        {
            System.Console.WriteLine("Reading the 1000 latest/newest events from EventStore with ReadAllEventsBackwardAsync");

            var readSlice = await _eventStoreConnection.ReadAllEventsBackwardAsync(Position.End, 1000, false);

            System.Console.WriteLine($"Number of events: {readSlice.Events.Length} with read direction: {readSlice.ReadDirection}, NextPositionToRead: {readSlice.NextPosition}");
        }

        private static async Task ReadStreamForward()
        {
            System.Console.WriteLine($"Reading 5 events from stream: {Stream} with ReadStreamEventsForwardAsync");

            var readSlice = await _eventStoreConnection.ReadStreamEventsForwardAsync(Stream, StreamPosition.Start, 5, false);

            System.Console.WriteLine($"Number of events: {readSlice.Events.Length} with read direction: {readSlice.ReadDirection}, NextEventToRead: {readSlice.NextEventNumber}, IsEndOfStream: {readSlice.IsEndOfStream}");
            
            foreach (var eventData in readSlice.Events)
            {
                System.Console.WriteLine($"Event: {eventData.Event.EventType}");
            }
        }

        private static async Task ReadStreamBackwards()
        {
            System.Console.WriteLine($"Reading 5 events from stream: {Stream} with ReadStreamEventsBackwardAsync");

            var readSlice = await _eventStoreConnection.ReadStreamEventsBackwardAsync(Stream, StreamPosition.End, 5, false);

            System.Console.WriteLine($"Number of events: {readSlice.Events.Length} with read direction: {readSlice.ReadDirection}, NextEventToRead: {readSlice.NextEventNumber}, IsEndOfStream: {readSlice.IsEndOfStream}");

            foreach (var eventData in readSlice.Events)
            {
                System.Console.WriteLine($"Event: {eventData.Event.EventType}");
            }
        }

        private static async Task ReadSingleEvent()
        {
            System.Console.WriteLine($"Reading event number 4 from stream: {Stream} with ReadEventAsync");

            var eventReadResult = await _eventStoreConnection.ReadEventAsync(Stream, 4, false);

            System.Console.WriteLine($"Result: {eventReadResult.Status}");
        }

        private static async Task CreateStreamData()
        {
            var result = await _eventStoreConnection.ConditionalAppendToStreamAsync(Stream, ExpectedVersion.NoStream, new[]
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
            return new EventData(Guid.NewGuid(), $"ReadEvent-{number}", false, null, null);
        }
    }
}
