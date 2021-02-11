using System.Net.Http;
using System.Threading.Tasks;
using EventStore.Client;
using EventStore.Examples.Infrastructure.ESdb6;
using EventStore.Examples.Infrastructure.Logging;
using Serilog;

namespace EventStore.Examples.Append.ESdb6.Console
{
    internal static class Program
    {
        private const string StreamName = "WriteStream-1";

        private static EventStoreClient _eventStoreClient;

        public static async Task Main(string[] args)
        {
            SerilogConsoleConfiguration.Configure();

            _eventStoreClient = EventStoreConnectionFactory.Create("esdb://127.0.0.1:2113");

            await RegularAppend();
            await RegularConditionalAppend();

            System.Console.ReadLine();
        }

        private static async Task RegularAppend()
        {
            try
            {
                Log.Information("Appending to {Stream} using AppendToStreamAsync", StreamName);

                var result = await _eventStoreClient.AppendToStreamAsync(StreamName, StreamState.NoStream, new []{ CreateEvent() });

                Log.Information("Result ok!, NextExpectedStreamRevision: {NextExpectedVersion}", result.NextExpectedStreamRevision);
            }
            catch (WrongExpectedVersionException exception)
            {
                Log.Error(exception, "Wrong version");
            }
        }

        private static async Task RegularConditionalAppend()
        {
            Log.Information("Appending to {Stream} using ConditionalAppendToStreamAsync", StreamName);

            StreamRevision expectedVersion = 0;
            var conditionalResult = await _eventStoreClient.ConditionalAppendToStreamAsync(StreamName, expectedVersion, new[] { CreateEvent() });

            Log.Information("Result: {Status}, NextExpectedVersion: {NextExpectedVersion}", conditionalResult.Status, conditionalResult.NextExpectedVersion);
        }

        private static EventData CreateEvent()
        {
            return new EventData(
                eventId: Uuid.NewUuid(),
                type: "AppendedEvent",
                data: null, 
                metadata: null);
        }
    }
}