using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Client;
using EventStore.Examples.Infrastructure.ESdb6;
using EventStore.Examples.Infrastructure.Logging;
using Serilog;

namespace ESdb6.Subscriptions.Console
{
    internal static class Program
    {
        private static EventStoreClient? _client;
        private static long _numberOfEvents = 0;
        private static Position? _endPosition;
        
        public static async Task Main(string[] args)
        {
            SerilogConsoleConfiguration.Configure();

            _client = EventStoreConnectionFactory.Create("esdb://127.0.0.1:2113");

            var endEvents = await _client.ReadAllAsync(Direction.Backwards, Position.End, 1).ToListAsync();
            _endPosition = endEvents.FirstOrDefault().OriginalPosition;

            await foreach (var resolvedEvent in _client.ReadAllAsync(Direction.Forwards, Position.Start, configureOperationOptions: options => options.TimeoutAfter = TimeSpan.MaxValue))
            {
                await ProcessEvent(resolvedEvent);
                
                if (resolvedEvent.OriginalPosition == _endPosition)
                {
                    Log.Information("LIVE, number of events {NumberOfEvents}", _numberOfEvents);
                    break;
                }
            }
            
            Log.Information("Starting subscription from {Position}", _endPosition);
            var subscription = await _client.SubscribeToAllAsync(
                _endPosition ?? Position.Start, 
                EventAppeared
                ,filterOptions: new SubscriptionFilterOptions(StreamFilter.Prefix("company-", "credit-"))
                );

            System.Console.ReadLine();
            
            Log.Information("Total number of events processed: {NumberOfEvents}", _numberOfEvents);
            subscription.Dispose();
        }

        private static async Task EventAppeared(StreamSubscription subscription, ResolvedEvent resolvedEvent, CancellationToken cancellationToken)
        {
            if (resolvedEvent.OriginalPosition == _endPosition)
                return;
            await ProcessEvent(resolvedEvent);
        }

        private static Task ProcessEvent(ResolvedEvent resolvedEvent)
        {
            if (_numberOfEvents != 0 && _numberOfEvents % 1000 == 0)
                Log.Information("Number of events processed {NumberOfEvents}!", _numberOfEvents);
            
            _numberOfEvents++;
            
            return Task.CompletedTask;
        }
    }
}