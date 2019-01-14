using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using static System.FormattableString;

namespace CrossPartitionAggregateFunctions
{
    public static class ItemCountByCustomer
    {
        private static readonly object staticLock = new object();
        private static readonly Uri collectionUri = UriFactory.CreateDocumentCollectionUri("TestDB", "Items");

        private static DocumentClient cosmosDbClient = null;

        [FunctionName("ItemCountByCustomer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {
            try
            {
                EnsureClient(context);
                string customer = req.Query["customer"];

                SqlQuerySpec querySpec = null;
                IDocumentQuery<dynamic> query = null;

                if (!String.IsNullOrWhiteSpace(customer))
                {
                    querySpec = new SqlQuerySpec(
                        String.Concat(
                            "SELECT VALUE COUNT(1) FROM Items i WHERE i.customer IN ('",
                            customer,
                            "')"));

                    query = cosmosDbClient.CreateDocumentQuery(
                        collectionUri,
                        querySpec,
                        new FeedOptions()
                        {
                            EnableCrossPartitionQuery = false,
                            PartitionKey = new PartitionKey(customer),
                            PopulateQueryMetrics = true,
                            MaxItemCount = 50,
                            MaxDegreeOfParallelism = 0,
                            MaxBufferedItemCount = 100
                        }).AsDocumentQuery();
                }
                else
                {
                    querySpec = new SqlQuerySpec("SELECT VALUE COUNT(1) FROM Items i");
                    query = cosmosDbClient.CreateDocumentQuery(
                        collectionUri,
                        querySpec,
                        new FeedOptions()
                        {
                            EnableCrossPartitionQuery = true,
                            PartitionKey = null,
                            PopulateQueryMetrics = true,
                            MaxItemCount = 50,
                            MaxDegreeOfParallelism = 0,
                            MaxBufferedItemCount = 100
                        }).AsDocumentQuery();
                }

                double totalRUs = 0;

                long count = 0;

                while (query.HasMoreResults)
                {
                    FeedResponse<dynamic> feedResponse = await query.ExecuteNextAsync();
                    Console.WriteLine(feedResponse.RequestCharge);
                    totalRUs += feedResponse.RequestCharge;
                    IReadOnlyDictionary<string, QueryMetrics> partitionIdToQueryMetrics = feedResponse.QueryMetrics;
                    foreach (KeyValuePair<string, QueryMetrics> kvp in partitionIdToQueryMetrics)
                    {
                        string partitionId = kvp.Key;
                        QueryMetrics queryMetrics = kvp.Value;
                        Console.WriteLine("{0}: {1}", partitionId, queryMetrics);
                    }

                    IEnumerator<dynamic> docEnumerator = feedResponse.GetEnumerator();
                    while (docEnumerator.MoveNext())
                    {
                        count += (long)docEnumerator.Current;
                    }
                }

                var responsePayload = new ResponseContract
                {
                    Count = count,
                    TotalRUs = totalRUs,
                };

                log.LogInformation("Count: {0}, Total RUs: {1}", count, totalRUs);

                return new OkObjectResult(JsonConvert.SerializeObject(responsePayload));
            }
            catch (Exception error)
            {
                return new ObjectResult(error.ToString()) { StatusCode = 500 };
            }
        }

        private static void EnsureClient(ExecutionContext context)
        {
            if (cosmosDbClient != null)
            {
                return;
            }

            lock (staticLock)
            {
                if (cosmosDbClient != null)
                {
                    return;
                }

                IConfigurationRoot config = new ConfigurationBuilder()
                    .SetBasePath(context.FunctionAppDirectory)
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

                cosmosDbClient = new DocumentClient(
                    connectionString.Endpoint,
                    connectionString.AuthKey,
                    CosmosDBConnectionString.DefaultPolicy);
            }
        }

        [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
        public class ResponseContract
        {
            [JsonProperty(Required = Required.Always)]
            public double TotalRUs
            {
                get; set;
            }

            [JsonProperty(Required = Required.Always)]
            public long Count
            {
                get; set;
            }
        }
    }
}
