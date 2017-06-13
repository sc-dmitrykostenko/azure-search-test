using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace SearchThroughput
{
    public class Monitor
    {
        private class Snapshot
        {
            public int count;

            public long time;

            public int searchableCount;

            public int requestCount;

            public long requestLatency;

            public int nthreads;

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

        public void Tick(bool showThroughput)
        {
            if (this.time.Elapsed.Ticks - this.s.time >= TimeSpan.FromSeconds(1).Ticks)
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

                StringBuilder line = new StringBuilder($"{stats.ts}: ");
                line.Append($"Documents: {stats.count} ");
                if (showThroughput)
                {
                    line.Append($"Current throughput: {stats.current} docs/s ");
                    line.Append($"Avg. throughput: {stats.average} docs/s ");
                }
                line.Append($"Searchable docs: {stats.searchable} ");
                line.Append($"Request latency: {stats.latency.TotalSeconds}s");
                Console.WriteLine(line);

                // shift
                this.ps = ns;
            }
        }

        public void EnterThread()
        {
            Interlocked.Increment(ref this.s.nthreads);
        }

        public void ExitThread()
        {
            Interlocked.Decrement(ref this.s.nthreads);
        }
    }
}