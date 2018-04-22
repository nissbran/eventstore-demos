namespace EventStore.Examples.Helpers.Configuration
{
    public interface IEventStoreClusterNode
    {
        int Number { get; }
        string IpAddress { get; }
        string HostName { get; }
        int ExternalPort { get; }

        bool HostNameSpecified { get; }
    }
}