using System;
using System.Collections.Generic;
using System.Net.Http;

using Newtonsoft.Json.Linq;

namespace SearchThroughput
{
    public class Api
    {
        readonly HttpClient client;

        private readonly string indexName;

        private string apiVersion = "2016-09-01";

        public Api(string url, string apiKey, string indexName)
        {
            this.indexName = indexName;
            this.client = new HttpClient();
            this.client.BaseAddress = new Uri(url);
            this.client.DefaultRequestHeaders.Add("Api-Key", apiKey);
        }

        public int GetSearchableCount(string query)
        {
            var docCountResponse = JObject.Parse(this.client.GetStringAsync($"/indexes/{this.indexName}/docs?api-version={this.apiVersion}&search={query}&$count=true&$top=0").Result);
            return docCountResponse["@odata.count"].Value<int>();
        }

        public void CreateIndex(object schema)
        {
            this.client.PutAsync($"/indexes/{this.indexName}?api-version={this.apiVersion}", schema.ToJsonBody())
                .Result.EnsureSuccessStatusCode();
        }

        public void DeleteIndex()
        {
            this.client.DeleteAsync($"/indexes/{this.indexName}?api-version={this.apiVersion}")
                .Result.EnsureSuccessStatusCode();
        }

        public void PostDocuments(List<object> documents)
        {
            var request = new { value = documents };
            this.client.PostAsync($"/indexes/test/docs/index?api-version={this.apiVersion}", request.ToJsonBody())
                .Result.EnsureSuccessStatusCode();
        }
    }
}