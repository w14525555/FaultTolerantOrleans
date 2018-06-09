using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Hosting;
using Orleans.Configuration;
using Utils;
using System.Net;
using GrainImplementation;
using Orleans;
using SystemInterfaces.Model;
using OrleansClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Tasks;
using Moq;
using SystemInterfaces;

namespace Test
{
    [TestClass]
    public class BatchManagerTest
    {
        private ISiloHost silo;
        private IClusterClient client;
        private IStreamSource source;
        private IStreamSource source2;
        private StatefulStreamObserver statefulStreamObserver;
        private static string joinedChannel = "general";
        private static string joinedChannel2 = "second";
        private static string userName = "You Wu";
        private const string NOT_EXIST = "Not Exist";
        private const string INITIAL_KEY = "initialKey";
        private const string INITIAL_VALUE = "initialValue";
        private static string[] members;
        private static StreamMessage barrierMsg = new StreamMessage(Constants.System_Key, Constants.Barrier_Value);
        private static StreamMessage commitMsg = new StreamMessage(Constants.System_Key, Constants.Barrier_Value);
        private static StreamMessage wordCountMessage1 = new StreamMessage("Word Count Example 1", "go go go follow me");
        private static StreamMessage wordCountMessage2 = new StreamMessage("Word Count Example 2", "restart the game");

        [TestInitialize]
        public async Task SetUpAsync()
        {
            await StartSilo();
            await StartClient();
        }

        [TestCleanup]
        public async Task CleanUp()
        {
            await StopClient();
            await StopSilo();
        }


        //Set Up Testing 
        [TestMethod]
        public void TestSiloAndClinetInitAsync()
        {
            Assert.AreEqual(client.IsInitialized, true);
        }

        [TestMethod]
        public async Task TestJoinRoom()
        {
            await SetUpSource();
            Assert.AreEqual(1, 1);
        }


        //Batch Processing Testsing 
        [TestMethod]
        public async Task TestEmtyBatchSentThenTheBatchIsReadForCommit()
        {
            await SetUpSource();
            barrierMsg.BatchID = 0;
            await source.ProduceMessageAsync(barrierMsg);
            var batchTracker = client.GetGrain<IBatchTracker>(Constants.Tracker);
            bool isCurrentBatchCompleted = await batchTracker.IsReadForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

        [TestMethod]
        public async Task TestMultipleSourcesReadForCommit()
        {
            await SetUpSource();
            await SetUpSource2();
            barrierMsg.BatchID = 0;
            await source.ProduceMessageAsync(barrierMsg);
            await source2.ProduceMessageAsync(barrierMsg);
            var batchTracker = client.GetGrain<IBatchTracker>(Constants.Tracker);
            bool isCurrentBatchCompleted = await batchTracker.IsReadForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

        //SetUp Functions 

        private Task StartSilo()
        {
            var builder = new SiloHostBuilder()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.ClusterId;
                    options.ServiceId = Constants.ServiceId;
                })
                .UseLocalhostClustering()
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(StreamSource).Assembly).WithReferences())
                //need to configure a grain storage called "PubSubStore" for using streaming with ExplicitSubscribe pubsub type
                .AddMemoryGrainStorage("PubSubStore")
                //Depends on your application requirements, you can configure your silo with other stream providers, which can provide other features, 
                //such as persistence or recoverability. For more information, please see http://dotnet.github.io/orleans/Documentation/Orleans-Streams/Stream-Providers.html
                .AddSimpleMessageStreamProvider(Constants.ChatRoomStreamProvider);

            silo = builder.Build();
            silo.StartAsync().Wait();
            return Task.CompletedTask;
        }

        private Task StopSilo()
        {
            silo.StopAsync().Wait();
            return Task.CompletedTask;
        }

        private async Task<Task> StartClient()
        {
            client = await GetClient();
            await client.Connect();
            
            return Task.CompletedTask;
        }

        private Task<IClusterClient> GetClient()
        {
            IClusterClient aClient = new ClientBuilder().Configure<ClusterOptions>(options =>
            {
                options.ClusterId = Constants.ClusterId;
                options.ServiceId = Constants.ServiceId;
            })
                        .UseLocalhostClustering()
                        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IStreamSource).Assembly).WithReferences())
                        //Depends on your application requirements, you can configure your client with other stream providers, which can provide other features, 
                        //such as persistence or recoverability. For more information, please see http://dotnet.github.io/orleans/Documentation/Orleans-Streams/Stream-Providers.html
                        .AddSimpleMessageStreamProvider(Constants.ChatRoomStreamProvider)
                        .Build();
            return Task.FromResult(aClient);
        }

        private Task StopClient()
        {
            client.Close().Wait();
            return Task.CompletedTask;
        }

        private async Task<Task> SetUpSource()
        {
            source = client.GetGrain<IStreamSource>(joinedChannel);
            var streamId = await source.Join(userName);
            var stream = client.GetStreamProvider(Constants.ChatRoomStreamProvider)
                .GetStream<StreamMessage>(streamId, Constants.CharRoomStreamNameSpace);
            //subscribe to the stream to receiver furthur messages sent to the chatroom
            Mock<ILogger> mockLogger = new Mock<ILogger>();
            statefulStreamObserver = new StatefulStreamObserver(mockLogger.Object);
            await stream.SubscribeAsync(statefulStreamObserver);
            members = await source.GetMembers();
            return Task.CompletedTask;
        }

        private async Task<Task> SetUpSource2()
        {
            source2 = client.GetGrain<IStreamSource>(joinedChannel2);
            var streamId = await source2.Join(userName);
            var stream = client.GetStreamProvider(Constants.ChatRoomStreamProvider)
                .GetStream<StreamMessage>(streamId, Constants.CharRoomStreamNameSpace);
            //subscribe to the stream to receiver furthur messages sent to the chatroom
            Mock<ILogger> mockLogger = new Mock<ILogger>();
            statefulStreamObserver = new StatefulStreamObserver(mockLogger.Object);
            await stream.SubscribeAsync(statefulStreamObserver);
            members = await source2.GetMembers();
            return Task.CompletedTask;
        }

    }
}
