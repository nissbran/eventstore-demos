using System.Collections.Generic;

namespace EventStore.Examples.Helpers.Configuration
{
    public interface IEventStoreClusterConfiguration
    {
        bool UseSsl { get; }

        IEnumerable<IEventStoreClusterNode> ClusterNodes { get; }
    }
}