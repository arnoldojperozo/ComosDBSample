using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace CosmosDbSample
{
    class Program
    {
        private string EndpointUrl = Environment.GetEnvironmentVariable("EndpointUrl");
        private string PrimaryKey = Environment.GetEnvironmentVariable("Primary");
        private CosmosClient cosmosClient;
        private Database database;
        private Container container;

        private string databaseId = "FamilyDatabase";
        private string containerId = "FamilyContainer";
        
        static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Beginning Operations - Cosmos DB");
                Program p = new Program();
                await p.GetStartedDemoAsync();
            }
            catch (CosmosException de)
            {
                Console.WriteLine("Error: {0}", de);
            }
            finally
            {
                Console.WriteLine("Press any key to Exit");
                Console.ReadKey();
            }
        }

        //Create Database if does not Exist
        private async Task CreateDatabaseAsync()
        {
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database; {0}\n",this.database.Id);
        }
        
        //Create Container if does not exist
        private async Task CreateContainerAsync()
        {
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/LastName");
            Console.WriteLine("Created Container: {0}\n",this.container.Id);
        }

        private async Task AddItemsToContainerAsync()
        {
            Family myFamily = new Family()
            {
                Id = "Andersen.1",
                LastName = "Andersen",
                Parents = new Parent[]
                {
                    new Parent {FirstName = "Thomas"},
                    new Parent {FirstName = "Mary Kay"}
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FirstName = "Henry",
                        Gender = "Male",
                        Grade = 5,
                        Pets = new Pet[]
                        {
                            new Pet {GivenName = "Fluffy"}
                        }
                    }
                },
                Address = new Address {State = "WA", County = "King", City = "Seattle"},
                IsRegistered = false
            };

            try
            {
                ItemResponse<Family> myFamilyResponse =
                    await this.container.CreateItemAsync<Family>(myFamily, new PartitionKey(myFamily.LastName));
                Console.WriteLine("Created Item in DB with Id: {0}. Operation Consumed {1} RUs.\n",
                    myFamilyResponse.Resource.Id, myFamilyResponse.RequestCharge);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                Console.WriteLine("Item in database with Id: {0} already exist\n", myFamily.Id);
            }
        }

        private async Task QueryItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'Andersen'";
            
            Console.WriteLine("Running query: {0}\n",sqlQueryText);
            
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<Family> queryResultSetIterator = this.container.GetItemQueryIterator<Family>(queryDefinition);
            
            List<Family> families = new List<Family>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (var family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine("\tRead {0}\n", family);
                }
            }
        }
        
        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();
            // Also valid: await this.cosmosClient.Databases["FamilyDatabase"].DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", this.databaseId);

            //Dispose of CosmosClient
            this.cosmosClient.Dispose();
        }
        
        public async Task GetStartedDemoAsync()
        {
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUrl, PrimaryKey);
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync();
            await this.AddItemsToContainerAsync();
            await this.QueryItemsAsync();
        }
    }
}