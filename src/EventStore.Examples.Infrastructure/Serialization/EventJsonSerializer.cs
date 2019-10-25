using System.Text;
using Newtonsoft.Json;

namespace EventStore.Examples.Infrastructure.Serialization
{
    public static class EventJsonSerializer
    {
        private static JsonSerializerSettings _settings;

        static EventJsonSerializer()
        {
            _settings = new JsonSerializerSettings();
        }

        public static byte[] SerializeObject(object eventObject)
        {
            var jsonString = JsonConvert.SerializeObject(eventObject);

            return Encoding.UTF8.GetBytes(jsonString);
        }
    }
}
