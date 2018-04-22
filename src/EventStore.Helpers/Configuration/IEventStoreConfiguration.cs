namespace EventStore.Examples.Helpers.Configuration
{
    public interface IEventStoreConfiguration
    {
        bool UseSingleNode { get; }

        string SingleNodeConnectionUri { get; }

        IEventStoreClusterConfiguration ClusterConfiguration { get; }
    }
}