using System;
using System.Net;
using System.Net.Http;
using EventStore.Client;

namespace EventStore.Examples.Infrastructure.ESdb6
{ 
    public static class EventStoreConnectionFactory
    {
        public static EventStoreClient Create(
            string connectionString = "esdb://127.0.0.1:2113,127.0.0.1:2123,127.0.0.1:2133?tls=true",
            string username = "admin",
            string password = "changeit",
            string connectionName = null)
        {
            
            var settings = EventStoreClientSettings.Create(connectionString);
            settings.CreateHttpMessageHandler = delegate { return new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback = delegate { return true; }
                };
            };
            settings.OperationOptions.TimeoutAfter = TimeSpan.FromSeconds(10);
            settings.DefaultCredentials = new UserCredentials(username, password);

            return new EventStoreClient(settings);
        }
    }
}