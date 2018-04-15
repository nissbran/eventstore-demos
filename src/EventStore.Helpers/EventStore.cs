namespace EventStore.Configuration
{
    //using System;
    //using System.Collections.Generic;
    //using System.Linq;
    //using System.Text;
    //using System.Threading.Tasks;
    //using Bank.Infrastructure.Domain;
    //using Bank.Infrastructure.EventStore;
    //using global::EventStore.ClientAPI;
    //using Newtonsoft.Json;
    //using Newtonsoft.Json.Serialization;

    //public class EventStore : ISnapshotEventStore
    //{
    //    private const long MinStreamVersion = 0;
    //    private const int ReadBatchSize = 2000;
    //    private const int WriteBatchSize = 4096;

    //    private readonly IEventStoreConnection _connection;
    //    private readonly Dictionary<string, IEventSchema> _eventSchemas = new Dictionary<string, IEventSchema>();
    //    private readonly JsonSerializerSettings _jsonSerializerSettings;

    //    public EventStore(IEventStoreConnection connection, IEnumerable<IEventSchema> eventSchemas)
    //    {
    //        _connection = connection;

    //        foreach (var schema in eventSchemas)
    //        {
    //            _eventSchemas.Add(schema.Name, schema);
    //        }

    //        _jsonSerializerSettings = new JsonSerializerSettings
    //        {
    //            ContractResolver = new CamelCasePropertyNamesContractResolver(),
    //            NullValueHandling = NullValueHandling.Ignore
    //        };
    //    }

    //    public async Task<IList<IDomainEvent>> GetEventsByStreamId(EventStreamId eventStreamId)
    //    {
    //        return await GetEventsFromStreamVersion(eventStreamId.StreamName, MinStreamVersion, eventStreamId.ResolveLinks);
    //    }

    //    public async Task<IList<IDomainEvent>> GetEventsBySnapshotStreamId(SnapshotEventStreamId snapshotEventStreamId)
    //    {
    //        var readSlice = await _connection.ReadStreamEventsBackwardAsync(
    //            stream: snapshotEventStreamId.SnapshotStreamName,
    //            start: StreamPosition.End, 
    //            count: 1, 
    //            resolveLinkTos: false);

    //        var streamEvents = new List<IDomainEvent>();

    //        if (readSlice.Status != SliceReadStatus.Success)
    //        {
    //            return await GetEventsFromStreamVersion(snapshotEventStreamId.StreamName, MinStreamVersion, snapshotEventStreamId.ResolveLinks);
    //        }
    //        else
    //        {
    //            var snapShotEvent = readSlice.Events[0];

    //            var dataAsJson = Encoding.UTF8.GetString(snapShotEvent.Event.Data);
    //            var snapShot = JsonConvert.DeserializeObject<Snapshot>(dataAsJson);

    //            streamEvents.Add(ConvertEventDataToDomainEvent(snapShotEvent));

    //            var events = await GetEventsFromStreamVersion(snapshotEventStreamId.StreamName, snapShot.SnapshotStreamVersion, snapshotEventStreamId.ResolveLinks);

    //            streamEvents.AddRange(events);

    //            return streamEvents;
    //        }
    //    }

    //    private async Task<IList<IDomainEvent>> GetEventsFromStreamVersion(string streamName, long streamVersion, bool resolveLinks)
    //    {
    //        var streamEvents = new List<ResolvedEvent>();

    //        StreamEventsSlice currentSlice;
    //        var nextSliceStart = streamVersion;
    //        do
    //        {
    //            currentSlice = await _connection.ReadStreamEventsForwardAsync(
    //                stream: streamName,
    //                start: nextSliceStart,
    //                count: ReadBatchSize,
    //                resolveLinkTos: resolveLinks);

    //            nextSliceStart = currentSlice.NextEventNumber;

    //            streamEvents.AddRange(currentSlice.Events);

    //        } while (!currentSlice.IsEndOfStream);

    //        return streamEvents.Select(ConvertEventDataToDomainEvent).ToList();
    //    }

    //    public async Task<StreamWriteResult> SaveEvents(EventStreamId eventStreamId, long streamVersion, List<IDomainEvent> events)
    //    {
    //        if (events.Any() == false)
    //            return new StreamWriteResult(-1);

    //        var commitId = Guid.NewGuid();

    //        var expectedVersion = streamVersion == 0 ? ExpectedVersion.NoStream : streamVersion - 1;
    //        var eventsToSave = events.Select(domainEvent => ToEventData(commitId, domainEvent));

    //        //if (eventsToSave.Length < WriteBatchSize)
    //        //{
    //        var result = await _connection.AppendToStreamAsync(
    //            stream: eventStreamId.ToString(),
    //            expectedVersion: expectedVersion,
    //            events: eventsToSave);

    //        return new StreamWriteResult(result.NextExpectedVersion);
    //        //}

    //        //using (var transaction = await _connection.StartTransactionAsync(eventStreamId.ToString(), expectedVersion))
    //        //{
    //        //    var position = 0;
    //        //    while (position < eventsToSave.Length)
    //        //    {
    //        //        var pageEvents = eventsToSave.Skip(position).Take(WriteBatchSize);
    //        //        await transaction.WriteAsync(pageEvents);
    //        //        position += WriteBatchSize;
    //        //    }

    //        //    var result = await transaction.CommitAsync();

    //        //    return new StreamWriteResult(result.NextExpectedVersion);
    //        //}
    //    }

    //    public async Task<StreamWriteResult> SaveSnapshot(SnapshotEventStreamId snapshotEventStreamId, IDomainEvent snapshot)
    //    {
    //        var result = await _connection.AppendToStreamAsync(
    //            snapshotEventStreamId.SnapshotStreamName,
    //            ExpectedVersion.Any,
    //            new[] { ToEventData(Guid.NewGuid(), snapshot) });

    //        return new StreamWriteResult(result.NextExpectedVersion);
    //    }

    //    private IDomainEvent ConvertEventDataToDomainEvent(ResolvedEvent resolvedEvent)
    //    {
    //        var metadataString = Encoding.UTF8.GetString(resolvedEvent.Event.Metadata);
    //        var eventString = Encoding.UTF8.GetString(resolvedEvent.Event.Data);

    //        //var metadata = new DomainMetaDataWrapper(metadataString);
    //        var metadata = JsonConvert.DeserializeObject<DomainMetadata>(metadataString, _jsonSerializerSettings);

    //        _eventSchemas.TryGetValue(metadata.Schema, out var schema);

    //        var eventType = schema.GetDomainEventType(resolvedEvent.Event.EventType);

    //        var domainEvent = (IDomainEvent)JsonConvert.DeserializeObject(eventString, eventType, _jsonSerializerSettings);
    //        domainEvent.StreamId = metadata.StreamId;
    //        domainEvent.Version = metadata.Version;

    //        return domainEvent;
    //    }

    //    private EventData ToEventData(Guid commitId, IDomainEvent domainEvent)
    //    {
    //        _eventSchemas.TryGetValue(domainEvent.Schema, out var schema);

    //        var definition = schema.GetEventDefinition(domainEvent);
    //        var eventId = Guid.NewGuid();

    //        var dataJson = JsonConvert.SerializeObject(domainEvent, _jsonSerializerSettings);
    //        var metadataJson = new DomainMetaDataWrapper
    //        {
    //            CorrelationId = commitId,
    //            CausationId = eventId,
    //            StreamId = domainEvent.StreamId,
    //            Version = definition.LatestVersion,
    //            Schema = domainEvent.Schema,
    //            Created = DateTimeOffset.UtcNow
    //        }.ToString();


    //        //    JsonConvert.SerializeObject(new DomainMetadata
    //        //{
    //        //    C
    //        //}, _jsonSerializerSettings);

    //        var data = Encoding.UTF8.GetBytes(dataJson);
    //        var metadata = Encoding.UTF8.GetBytes(metadataJson);

    //        return new EventData(eventId, definition.EventName, true, data, metadata);
    //    }

    //    private class Snapshot : ISnapshot
    //    {
    //        public long SnapshotStreamVersion { get; set; }
    //    }
   // }
}