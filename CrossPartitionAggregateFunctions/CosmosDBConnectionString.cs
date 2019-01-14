using System;
using System.Collections.Generic;
using System.Globalization;

namespace CrossPartitionAggregateFunctions
{
    public class CosmosDBConnectionString
    {
        private const string AccountEndpointKey = "AccountEndpoint";
        private const string AccountKeyKey = "AccountKey";
        private static readonly HashSet<string> RequireSettings =
            new HashSet<string>(new[] { AccountEndpointKey, AccountKeyKey }, StringComparer.OrdinalIgnoreCase);

        public CosmosDBConnectionString(Uri endpoint, string authKey)
        {
            if (endpoint == null)
            {
                throw new ArgumentNullException(nameof(endpoint));
            }

            if (String.IsNullOrWhiteSpace(authKey))
            {
                throw new ArgumentNullException(nameof(authKey));
            }

            this.Endpoint = endpoint;
            this.AuthKey = authKey;
        }

        public string AuthKey { get; set; }

        public Uri Endpoint { get; private set; }

        public static CosmosDBConnectionString Parse(string connectionString)
        {
            CosmosDBConnectionString ret;

            if (String.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (ParseImpl(connectionString, out ret, err => { throw new FormatException(err); }))
            {
                return ret;
            }

            throw new ArgumentException($"Connection string was not able to be parsed into a document client.");
        }

        public static bool TryParse(string connectionString, out CosmosDBConnectionString parsedConnectionString)
        {
            if (String.IsNullOrWhiteSpace(connectionString))
            {
                parsedConnectionString = null;
                return false;
            }

            try
            {
                return ParseImpl(connectionString, out parsedConnectionString, err => { });
            }
            catch (Exception)
            {
                parsedConnectionString = null;
                return false;
            }
        }

        private static bool ParseImpl(string connectionString, out CosmosDBConnectionString parsedConnectionString, Action<string> error)
        {
            IDictionary<string, string> settings = ParseStringIntoSettings(connectionString, error);

            if (settings == null)
            {
                parsedConnectionString = null;
                return false;
            }

            if (!RequireSettings.IsSubsetOf(settings.Keys))
            {
                parsedConnectionString = null;
                return false;
            }

            parsedConnectionString = new CosmosDBConnectionString(
                new Uri(settings[AccountEndpointKey]), settings[AccountKeyKey]);
            return true;
        }

        /// <summary>
        /// Tokenizes input and stores name value pairs.
        /// </summary>
        /// <param name="connectionString">The string to parse.</param>
        /// <param name="error">Error reporting delegate.</param>
        /// <returns>Tokenized collection.</returns>
        private static IDictionary<string, string> ParseStringIntoSettings(string connectionString, Action<string> error)
        {
            IDictionary<string, string> settings = new Dictionary<string, string>();
            string[] splitted = connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (string nameValue in splitted)
            {
                string[] splittedNameValue = nameValue.Split(new char[] { '=' }, 2);

                if (splittedNameValue.Length != 2)
                {
                    error("Settings must be of the form \"name=value\".");
                    return null;
                }

                if (settings.ContainsKey(splittedNameValue[0]))
                {
                    error(String.Format(CultureInfo.InvariantCulture, "Duplicate setting '{0}' found.", splittedNameValue[0]));
                    return null;
                }

                settings.Add(splittedNameValue[0], splittedNameValue[1]);
            }

            return settings;
        }
    }
}
