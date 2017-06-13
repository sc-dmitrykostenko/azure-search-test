using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace SearchThroughput
{
    class Program
    {
        public static void Main(string[] args)
        {
            var documentSizeKiB = 0.5;
            int nthreads = 4;
            int maxDocumentCount = (int)((10 / documentSizeKiB + 1) * 10000);

            Console.WriteLine($"{DateTime.UtcNow}: {maxDocumentCount} docs @ {documentSizeKiB}KiB, {nthreads} threads");

            int documentSize = (int)(documentSizeKiB * 1024);

            int documentCount = 0;
            Stopwatch time = new Stopwatch();
            time.Start();

#if S3
            var api = new Api("https://dk-test-pref-1.search.windows.net", "182625CF80F4413FFDC04FD35A4437F9", "test");
#else
            var api = new Api("https://dk-test-perf-s1.search.windows.net", "B0319F590B98743B3433B9C34BF2FE7B", "test");
#endif
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
            Task[] tasks = new Task[nthreads];
            var cts = new CancellationTokenSource();

            string uniqueId = Guid.NewGuid().ToString("n");
            var monitor = new Monitor(uniqueId, api);

            for (int t = 0; t < tasks.Length; t++)
            {
                tasks[t] = Task.Factory.StartNew(
                    () =>
                    {
                        monitor.EnterThread();
                        try
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
                                            Content = generator.BuildText(documentSize, 10, 2),
                                            Tag = uniqueId,
                                        });
                                }

                                var start = time.Elapsed;
                                api.PostDocuments(documents);
                                monitor.AddRequest(time.Elapsed - start);
                                monitor.DocumentsAdded(documents.Count);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Exception: {e}");
                        }
                        finally
                        {
                            monitor.ExitThread();
                        }
                    },
                    cts.Token,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Current);
            }

            while (monitor.DocumentCount < maxDocumentCount)
            {
                Thread.Sleep(TimeSpan.FromSeconds(10));
                monitor.Tick(true);
            }

            cts.Cancel();
            Console.WriteLine("Crawling stopped, waiting for indexer...");
            while (monitor.SearchableDocumentCount < monitor.DocumentCount)
            {
                Thread.Sleep(TimeSpan.FromSeconds(10));
                monitor.Tick(false);
            }
            Console.WriteLine("Done");
            Task.WaitAll(tasks);
        }
    }
}
