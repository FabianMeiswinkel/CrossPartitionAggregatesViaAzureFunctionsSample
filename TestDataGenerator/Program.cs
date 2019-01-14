using System;
using System.Configuration;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace TestDataGenerator
{
    internal static class Program
    {
        private const string ConnectionStringName = "CosmosDB";

        private static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            try
            {
                var config = new CosmosConfiguration(
                    ConfigurationManager.ConnectionStrings[ConnectionStringName].ConnectionString);

                using (var client = new CosmosClient(config))
                {
                    CosmosDatabase db = (await client.Databases.CreateDatabaseIfNotExistsAsync(
                        "TestDB",
                        400)).Database;

                    CosmosContainer collection = (await db.Containers.CreateContainerIfNotExistsAsync(
                        new CosmosContainerSettings(
                            "Items",
                            "/customer"))).Container;

                    for (int i = 0; i < 1000; i++)
                    {
                        var newRecord = Product.NewRandom();
                        await collection.Items.CreateItemAsync(
                            newRecord.PartitionKey,
                            newRecord);

                        Console.WriteLine(i);
                    }
                }
            }
            catch (Exception error)
            {
                Console.WriteLine("EXCEPTION: {0}", error);
            }

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
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
                    Customer = rnd.Next(20).ToString(CultureInfo.InvariantCulture),
                    ProductCode = rnd.Next(20).ToString(CultureInfo.InvariantCulture),
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
