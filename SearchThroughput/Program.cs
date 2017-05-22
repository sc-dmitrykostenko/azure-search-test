using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace SearchThroughput
{
    public class WordGenerator
    {
        Random rng = new Random(15);

        string alphabet = "abcdefghijklmnopqrstuvwxyz";

        public WordGenerator()
        {
        }

        public WordGenerator(string alphabet)
        {
            this.alphabet = alphabet;
        }

        public string NextWord(int length)
        {
            char[] chars = new char[length];

            for (int i = 0; i < chars.Length; i++)
            {
                chars[i] = this.alphabet[rng.Next(this.alphabet.Length)];
            }

            return new string(chars);
        }

        public string BuildText(int length, int wordLength, int variation)
        {
            StringBuilder builder = new StringBuilder(length + wordLength);
            while (builder.Length < length)
            {
                if (builder.Length > 0)
                {
                    builder.Append(" ");
                }
                builder.Append(this.NextWord(this.rng.Next(wordLength - variation, wordLength + variation)));
            }

            return builder.ToString();
        }
    }

    public static class Ext
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

    public class Monitor
    {
        private class Snapshot
        {
            public int count;

            public long time;

            public int searchableCount;

            public int requestCount;

            public long requestLatency;

            public Snapshot Clone()
            {
                return (Snapshot) this.MemberwiseClone();
            }
        }

        private Snapshot s;

        private Snapshot ps;

        private readonly Stopwatch time;

        private string tag;

        private Api api;

        public Monitor(string tag, Api api)
        {
            this.time = new Stopwatch();
            this.time.Start();
            this.s = new Snapshot { count = 0, time = this.time.Elapsed.Ticks, searchableCount = 0, requestCount = 0, requestLatency = 0 };
            this.ps = this.s.Clone();
            this.tag = tag;
            this.api = api;
        }

        public void DocumentsAdded(int count)
        {
            Interlocked.Add(ref this.s.count, count);
        }

        public void AddRequest(TimeSpan latency)
        {
            Interlocked.Increment(ref this.s.requestCount);
            Interlocked.Add(ref this.s.requestLatency, latency.Ticks);
        }

        public int DocumentCount => this.s.count;

        public int SearchableDocumentCount => this.s.searchableCount;

        public void Tick()
        {
            if (this.time.Elapsed.Ticks - s.time >= TimeSpan.FromSeconds(1).Ticks)
            {
                this.s.searchableCount = this.api.GetSearchableCount($"tag:{this.tag}");

                // new snapshot
                var ns = this.s.Clone();
                ns.time = this.time.Elapsed.Ticks;

                var stats =
                    new
                    {
                        ts = new TimeSpan(ns.time),
                        count = ns.count,
                        current = (ns.count - this.ps.count)/new TimeSpan(ns.time - this.ps.time).TotalSeconds,
                        average = ns.count/new TimeSpan(ns.time).TotalSeconds,
                        searchable = ns.searchableCount,
                        latency = new TimeSpan(ns.requestCount == 0 ? 0L : ns.requestLatency / ns.requestCount),
                    };
                Console.WriteLine($"{stats.ts}: Documents {stats.count}, Avg. throughput: {stats.average} docs/sec, Searchable Docs: {stats.searchable}, Request Latency: {stats.latency.TotalSeconds} s");
                this.ps = ns;
            }
        }
    }

    class Program
    {
        public static void Main(string[] args)
        {

            int documentCount = 0;
            Stopwatch time = new Stopwatch();
            time.Start();

            var api = new Api("https://dk-commerce-1-as.search.windows.net", "D2CE108F65DDB516F41BE8833DBEC265", "test");

            var schema =
                new
                {
                    Fields =
                    new[]
                    {
                        new { Name = "id", Key = true, Type = "Edm.String", },
                        new { Name = "content", Key = false, Type = "Edm.String", },
                        new { Name = "tag", Key = false, Type = "Edm.String", },
                    }
                };
            Console.WriteLine("Creating index...");
            api.CreateIndex(schema);

            Console.WriteLine("Posting documents...");
            int nthreads = 4;
            Task[] tasks = new Task[nthreads];
            var cts = new CancellationTokenSource();

            string uniqueId = Guid.NewGuid().ToString("n");
            var monitor = new Monitor(uniqueId, api);

            for (int t = 0; t < tasks.Length; t++)
            {
                tasks[t] = Task.Factory.StartNew(
                    () =>
                    {
                        var generator = new WordGenerator();

                        while (!cts.IsCancellationRequested)
                        {
                            List<object> documents = new List<object>();

                            for (int i = 0; i < 1000; i++)
                            {
                                documents.Add(
                                    new
                                    {
                                        Id = Guid.NewGuid().ToString("n"),
                                        Content = generator.BuildText(10240, 10, 2),
                                        Tag = uniqueId,
                                    });
                            }

                            var start = time.Elapsed;
                            api.PostDocuments(documents);
                            monitor.AddRequest(time.Elapsed - start);
                            monitor.DocumentsAdded(documents.Count);
                        }
                    },
                    cts.Token,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Current);
            }

            while (monitor.DocumentCount < 10000)
            {
                Thread.Sleep(TimeSpan.FromSeconds(10));
                monitor.Tick();
            }

            cts.Cancel();
            Console.WriteLine("Crawling stopped, waiting for indexer...");
            while (monitor.SearchableDocumentCount < monitor.DocumentCount)
            {
                Thread.Sleep(TimeSpan.FromSeconds(10));
                monitor.Tick();
            }
            Console.WriteLine("Done");
            Task.WaitAll(tasks);
        }
    }
}
