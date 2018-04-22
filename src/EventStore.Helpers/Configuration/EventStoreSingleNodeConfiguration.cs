namespace EventStore.Examples.Helpers.Configuration
{
    public class EventStoreSingleNodeConfiguration : IEventStoreConfiguration
    {
        public bool UseSingleNode { get; }
        public string SingleNodeConnectionUri { get; }
        public IEventStoreClusterConfiguration ClusterConfiguration { get; }

        public EventStoreSingleNodeConfiguration() : this(1113)
        {
        }

        public EventStoreSingleNodeConfiguration(int port)
        {
            UseSingleNode = true;
            SingleNodeConnectionUri = $"tcp://localhost:{port}";
            ClusterConfiguration = null;
        }
    }
}