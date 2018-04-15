namespace EventStore.Examples
{
    using EventStore.ClientAPI;
    using EventStore.ClientAPI.Exceptions;
    using EventStore.ClientAPI.SystemData;
    using System;
    using System.Net;
    using System.Threading.Tasks;

    class Program
    {
        private const string Username = "admin";
        private const string Password = "changeit";

        private const string Stream = "WriteStream-1";

        private static IEventStoreConnection _eventStoreConnection;

        public static async Task Main(string[] args)
        {
            var connectionSettings = ConnectionSettings.Create().SetDefaultUserCredentials(new UserCredentials(Username, Password));

            _eventStoreConnection = EventStoreConnection.Create(connectionSettings, new IPEndPoint(IPAddress.Loopback, 1113));

            await _eventStoreConnection.ConnectAsync();

            await RegularAppend();
            await RegularConditionalAppend();
            await AppendWithTransaction();

            Console.ReadLine();
        }

        private static async Task RegularAppend()
        {
            try
            {
                Console.WriteLine($"Appending to {Stream} using AppendToStreamAsync");

                var result = await _eventStoreConnection.AppendToStreamAsync(Stream, ExpectedVersion.EmptyStream, CreateEvent());

                Console.WriteLine($"Result ok!, NextExpectedVersion: {result.NextExpectedVersion}");
            }
            catch (WrongExpectedVersionException exception)
            {
                Console.WriteLine(exception);
            }
        }

        private static async Task RegularConditionalAppend()
        {
            Console.WriteLine($"Appending to {Stream} using ConditionalAppendToStreamAsync");

            var conditionalResult = await _eventStoreConnection.ConditionalAppendToStreamAsync(Stream, 0, new[] { CreateEvent() });

            Console.WriteLine($"Result: {conditionalResult.Status}, NextExpectedVersion: {conditionalResult.NextExpectedVersion}");
        }

        private static async Task AppendWithTransaction()
        {
            try
            {
                Console.WriteLine($"Appending to {Stream} using Transaction");

                using (var transaction = await _eventStoreConnection.StartTransactionAsync(Stream, 1))
                {
                    await transaction.WriteAsync(CreateEvent());
                    await transaction.WriteAsync(CreateEvent());

                    var result = await transaction.CommitAsync();

                    Console.WriteLine($"Result ok!, NextExpectedVersion: {result.NextExpectedVersion}");
                }
            }
            catch (WrongExpectedVersionException exception)
            {
                Console.WriteLine(exception);
            }
        }

        private static EventData CreateEvent()
        {
            return new EventData(Guid.NewGuid(), "AppendEvent", false, null, null);
        }
    }
}
