using System;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;

namespace EventStore.Examples.Infrastructure.ESdb5.Configuration
{ 
    public static class EventStoreConnectionFactory
    {
        public static IEventStoreConnection Create(
            string connectionString,
            string username,
            string password,
            ILogger customLogger = null,
            string connectionName = null)
        {
            var connectionSettings = ConnectionSettings.Create()
                .FailOnNoServerResponse()
                .KeepReconnecting()
                .KeepRetrying()
                .SetMaxDiscoverAttempts(int.MaxValue)
                .SetHeartbeatTimeout(TimeSpan.FromSeconds(5))
                .SetGossipTimeout(TimeSpan.FromSeconds(5))
                .SetDefaultUserCredentials(new UserCredentials(username, password));

            if (customLogger != null)
                connectionSettings.UseCustomLogger(customLogger);

            return EventStoreConnection.Create(connectionString, connectionSettings, connectionName);
        }
    }
}