using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CrossPartitionAggregateFunctions;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using static System.FormattableString;

namespace TestDataGenerator
{
    internal static class Program
    {
        private const int BatchCount = 10;
        private const int DocumentCountPerBatch = 1000;
        private const string DatabaseId = "TestDB";
        private const string CollectionId = "Items";

        private static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            try
            {
                DocumentClient cosmosDbClient = NewClient();

                // Set retry options high during initialization (default values).
                cosmosDbClient.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
                cosmosDbClient.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

                DocumentCollection collection = GetCollectionIfExists(
                    cosmosDbClient,
                    DatabaseId,
                    CollectionId);

                IBulkExecutor bulkExecutor = new BulkExecutor(
                    cosmosDbClient,
                    collection);
                await bulkExecutor.InitializeAsync();

                // Set retries to 0 to pass complete control to bulk executor.
                cosmosDbClient.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
                cosmosDbClient.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

                long numberOfDocumentsToGenerate = BatchCount * DocumentCountPerBatch;

                BulkImportResponse bulkImportResponse = null;
                long totalNumberOfDocumentsInserted = 0;
                double totalRequestUnitsConsumed = 0;
                double totalTimeTakenSec = 0;

                var tokenSource = new CancellationTokenSource();
                CancellationToken token = tokenSource.Token;

                for (int i = 0; i < BatchCount; i++)
                {
                    // Generate documents to import.
                    var batch = new Product[DocumentCountPerBatch];
                    for (int n = 0; n < DocumentCountPerBatch; n++)
                    {
                        batch[n] = Product.NewRandom();
                    }

                    // Invoke bulk import API.
                    var tasks = new List<Task>
                    {
                        Task.Run(async () =>
                        {
                            Console.WriteLine("Executing bulk import for batch {0}", i);
                            do
                            {
                                try
                                {
                                    bulkImportResponse = await bulkExecutor.BulkImportAsync(
                                        documents: batch,
                                        enableUpsert: true,
                                        disableAutomaticIdGeneration: true,
                                        maxConcurrencyPerPartitionKeyRange: null,
                                        maxInMemorySortingBatchSize: null,
                                        cancellationToken: token);
                                }
                                catch (DocumentClientException de)
                                {
                                    Console.WriteLine("Document client exception: {0}", de);
                                    break;
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Exception: {0}", e);
                                    break;
                                }
                            } while (bulkImportResponse.NumberOfDocumentsImported < DocumentCountPerBatch);

                            Console.WriteLine(String.Format("\nSummary for batch {0}:", i));
                            Console.WriteLine("--------------------------------------------------------------------- ");
                            Console.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                                bulkImportResponse.NumberOfDocumentsImported,
                                Math.Round(bulkImportResponse.NumberOfDocumentsImported / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                                Math.Round(bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                                bulkImportResponse.TotalTimeTaken.TotalSeconds));
                            Console.WriteLine(String.Format("Average RU consumption per document: {0}",
                                (bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.NumberOfDocumentsImported)));
                            Console.WriteLine("---------------------------------------------------------------------\n ");

                            totalNumberOfDocumentsInserted += bulkImportResponse.NumberOfDocumentsImported;
                            totalRequestUnitsConsumed += bulkImportResponse.TotalRequestUnitsConsumed;
                            totalTimeTakenSec += bulkImportResponse.TotalTimeTaken.TotalSeconds;
                        },
                    token),

                        Task.Run(() =>
                        {
                            char ch = Console.ReadKey(true).KeyChar;
                            if (ch == 'c' || ch == 'C')
                            {
                                tokenSource.Cancel();
                                Console.WriteLine("\nTask cancellation requested.");
                            }
                        })
                    };

                    await Task.WhenAll(tasks);
                }

                Console.WriteLine("Overall summary:");
                Console.WriteLine("--------------------------------------------------------------------- ");
                Console.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                    totalNumberOfDocumentsInserted,
                    Math.Round(totalNumberOfDocumentsInserted / totalTimeTakenSec),
                    Math.Round(totalRequestUnitsConsumed / totalTimeTakenSec),
                    totalTimeTakenSec));
                Console.WriteLine(String.Format("Average RU consumption per document: {0}",
                    (totalRequestUnitsConsumed / totalNumberOfDocumentsInserted)));
                Console.WriteLine("--------------------------------------------------------------------- ");

                Console.WriteLine("\nPress any key to exit.");
                Console.ReadKey();
            }
            catch (Exception error)
            {
                Console.WriteLine("EXCEPTION: {0}", error);
            }

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }

        private static DocumentClient NewClient()
        {
            IConfigurationRoot config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string connectionStringValue = config.GetConnectionString(CosmosDBConnectionString.KeyName);

            if (String.IsNullOrWhiteSpace(connectionStringValue))
            {
                throw new InvalidOperationException(
                    Invariant($"Connection string '{CosmosDBConnectionString.KeyName}' has not been defined."));
            }

            var connectionString = CosmosDBConnectionString.Parse(connectionStringValue);

            return new DocumentClient(
                    connectionString.Endpoint,
                    connectionString.AuthKey,
                    CosmosDBConnectionString.DefaultPolicy);
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested collection.</returns>
        private static DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested database.</returns>
        private static Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
        public class Product : IDocument
        {
            private static Random rnd = new Random();

            public Product()
            {
                this.Id = Guid.NewGuid().ToString("N");
            }

            [JsonProperty(Required = Required.Always)]
            public string Customer
            {
                get; set;
            }

            [JsonProperty(PropertyName = "_etag", DefaultValueHandling = DefaultValueHandling.Ignore)]
            public string ETag
            {
                get; set;
            }

            [JsonProperty(Required = Required.Always)]
            public string Id
            {
                get; private set;
            }

            [JsonIgnore]
            public string PartitionKey
            {
                get
                {
                    return this.Customer;
                }
            }

            [JsonProperty(Required = Required.Always)]
            public string ProductCode
            {
                get; set;
            }

            public static Product NewRandom()
            {
                return new Product
                {
                    Customer = rnd.Next(999).ToString(CultureInfo.InvariantCulture),
                    ProductCode = rnd.Next(999).ToString(CultureInfo.InvariantCulture),
                };
            }
        }

        public interface IDocument
        {
            string ETag
            {
                get;
            }

            string Id
            {
                get;
            }

            string PartitionKey
            {
                get;
            }
        }
    }
}
