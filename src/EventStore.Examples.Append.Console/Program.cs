using System;
using System.Net;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace EventStore.Examples.Append.Console
{
    internal static class Program
    {
        private const string Username = "admin";
        private const string Password = "changeit";

        private const string Stream = "WriteStream-1";

        private static IEventStoreConnection _eventStoreConnection;

        public static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.Console(theme: AnsiConsoleTheme.Code)
                .CreateLogger();
            
            var connectionSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials(Username, Password));

            _eventStoreConnection = EventStoreConnection.Create(connectionSettings, new IPEndPoint(IPAddress.Loopback, 1113));

            await _eventStoreConnection.ConnectAsync();

            await RegularAppend();
            await RegularConditionalAppend();
            await AppendWithTransaction();

            System.Console.ReadLine();
        }

        private static async Task RegularAppend()
        {
            try
            {
                Log.Information("Appending to {Stream} using AppendToStreamAsync", Stream);

                var result = await _eventStoreConnection.AppendToStreamAsync(Stream, ExpectedVersion.EmptyStream, CreateEvent());

                Log.Information("Result ok!, NextExpectedVersion: {NextExpectedVersion}", result.NextExpectedVersion);
            }
            catch (WrongExpectedVersionException exception)
            {
                System.Console.WriteLine(exception);
            }
        }

        private static async Task RegularConditionalAppend()
        {
            Log.Information("Appending to {Stream} using ConditionalAppendToStreamAsync", Stream);

            var conditionalResult = await _eventStoreConnection.ConditionalAppendToStreamAsync(Stream, 0, new[] { CreateEvent() });

            Log.Information("Result: {Status}, NextExpectedVersion: {NextExpectedVersion}", conditionalResult.Status, conditionalResult.NextExpectedVersion);
        }

        private static async Task AppendWithTransaction()
        {
            try
            {
                Log.Information("Appending to {Stream} using Transaction", Stream);

                using (var transaction = await _eventStoreConnection.StartTransactionAsync(Stream, 1))
                {
                    await transaction.WriteAsync(CreateEvent());
                    await transaction.WriteAsync(CreateEvent());

                    var result = await transaction.CommitAsync();

                    Log.Information("Result ok!, NextExpectedVersion: {NextExpectedVersion}", result.NextExpectedVersion);
                }
            }
            catch (WrongExpectedVersionException exception)
            {
                Log.Error(exception, "Wrong expected version");
            }
        }

        private static EventData CreateEvent()
        {
            return new EventData(Guid.NewGuid(), "AppendEvent", false, null, null);
        }
    }
}
