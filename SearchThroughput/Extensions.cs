using System.Net.Http;
using System.Text;

using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace SearchThroughput
{
    public static class Extensions
    {
        public static HttpContent ToJsonBody(this object instance)
        {
            var settings = new JsonSerializerSettings()
                           {
                               Formatting = Formatting.Indented,
                               ContractResolver = new CamelCasePropertyNamesContractResolver()
                           };
            return new StringContent(JsonConvert.SerializeObject(instance, settings), Encoding.UTF8, "application/json");
        }
    }
}