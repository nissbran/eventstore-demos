using Newtonsoft.Json;
using System.Text;

namespace EventStore.Examples.Helpers.Serialization
{
    public static class EventJsonDeserializer
    {
        private static JsonSerializerSettings _settings;

        static EventJsonDeserializer()
        {
            _settings = new JsonSerializerSettings();
        }

        public static T DeserializeObject<T>(byte[] eventData)
        {
            var jsonString = Encoding.UTF8.GetString(eventData);

            return JsonConvert.DeserializeObject<T>(jsonString);
        }
    }
}
