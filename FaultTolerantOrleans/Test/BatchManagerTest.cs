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
using System.Threading;

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
        private string[] members;
        private StreamMessage barrierMsg = new StreamMessage(Constants.System_Key, Constants.Barrier_Value);
        private StreamMessage commitMsg = new StreamMessage(Constants.System_Key, Constants.Commit_Value);
        private StreamMessage wordCountMessage1 = new StreamMessage("Word Count Example 1", "go go go follow me");
        private StreamMessage wordCountMessage2 = new StreamMessage("Word Count Example 2", "restart the game");

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


        //Batch Processing Tests
        [TestMethod]
        public async Task TestEmtyBatchSentThenTheBatchIsReadForCommit()
        {
            await SetUpSource();
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.SendBarrier();
            var batchTracker = client.GetGrain<IBatchTracker>(Constants.Tracker);
            Thread.Sleep(100);
            bool isCurrentBatchCompleted = await batchTracker.IsReadyForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

        [TestMethod]
        public async Task TestMultipleSourcesReadyForCommit()
        {
            await SetUpSource();
            await SetUpSource2();
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.SendBarrier();
            var batchTracker = client.GetGrain<IBatchTracker>(Constants.Tracker);
            Thread.Sleep(100);
            bool isCurrentBatchCompleted = await batchTracker.IsReadyForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

        //Commit Tests
        [TestMethod]
        public async Task TestInitialCommitedBatchID()
        {
            await SetUpSource();
            //Commit the batch 0
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            int commitedBatchID = await batchCoordinator.GetCommittedBatchID();
            Assert.AreEqual(-1, commitedBatchID);
        }

        [TestMethod]
        public async Task TestSendCommitMessageThenCommmitIsSuccessful()
        {
            await SetUpSource();
            //Commit the batch 0
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.StartCommit(0);
            var batchTracker = client.GetGrain<IBatchTracker>(Constants.Tracker);
            Thread.Sleep(100);
            bool isCommitCompleted = await batchTracker.IsCommitSuccess(0);
            Assert.AreEqual(true, isCommitCompleted);
        }

        //StateManagement Tests
        [TestMethod]
        public async Task TestWordCountStateInDownStreamGrains()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            int count = await source.GetState(new StreamMessage(wordCountMessage1.Key, "go"));
            Assert.AreEqual(3, count);
        }

        [TestMethod]
        public async Task TestWordCountStateInDownStreamGrainsOtherWord()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            int count = await source.GetState(new StreamMessage(wordCountMessage1.Key, "me"));
            Assert.AreEqual(1, count);
        }

        //Message buffer Tests
        [TestMethod]
        public async Task TestMessageBufferTheMessage()
        {
            await SetUpSource();
            wordCountMessage1.BatchID = 2;
            wordCountMessage1.messageType = MessageType.Test;
            await source.ProduceMessageAsync(wordCountMessage1);
            int count = await source.GetState(new StreamMessage(wordCountMessage1.Key, "me"));
            Assert.AreEqual(-2, count);
        }

        [TestMethod]
        public async Task TestMessagesBufferWillExecuteAfterCommit()
        {
            await SetUpSource();
            wordCountMessage1.BatchID = 1;
            wordCountMessage1.messageType = MessageType.Test;
            await source.ProduceMessageAsync(wordCountMessage1);
            //Commit the batch 0
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.StartCommit(0);
            int count = await source.GetState(new StreamMessage(wordCountMessage1.Key, "me"));
            Assert.AreEqual(1, count);
        }

        //Reverse Log Tests
        [TestMethod]
        public async Task TestReverseLogSaveTheStateOfLastBatch()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            int count = await source.GetStateInReverseLog(new StreamMessage(wordCountMessage1.Key, "go"));
            Assert.AreEqual(0, count);
        }

        [TestMethod]
        public async Task TestReverseLogClearsOnCommit()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.StartCommit(0);
            int count = await source.GetStateInReverseLog(new StreamMessage(wordCountMessage1.Key, "go"));
            Assert.AreEqual(-2, count);
        }

        //Incremental Log Tests
        [TestMethod]
        public async Task TestIncrementalLogSaveTheLatestState()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            int count = await source.GetStateInIncrementalLog(new StreamMessage(wordCountMessage1.Key, "go"));
            Assert.AreEqual(3, count);
        }

        [TestMethod]
        public async Task TestIncrementalLogClearAfterCommit()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.StartCommit(0);
            int count = await source.GetStateInIncrementalLog(new StreamMessage(wordCountMessage1.Key, "go"));
            Assert.AreEqual(-2, count);
        }

        //Recovery Tests
        [TestMethod]
        public async Task TestRecoveyFromReverseLogAfterOneBatch()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.SendBarrier();
            await source.ProduceMessageAsync(wordCountMessage1);
            int count = await source.GetState(new StreamMessage(wordCountMessage1.Key, "me"));
            Assert.AreEqual(2, count);
            await batchCoordinator.StartRecovery();
            int countAfterRecovery = await source.GetState(new StreamMessage(wordCountMessage1.Key, "me"));
            Assert.AreEqual(1, countAfterRecovery);
        }

        [TestMethod]
        public async Task TestRecoveyFromReverseLog()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.StartRecovery();
            int count = await source.GetState(new StreamMessage(wordCountMessage1.Key, "me"));
            Assert.AreEqual(-2, count);
        }

        [TestMethod]
        public async Task TestReverseLogClearAfterRevert()
        {
            await SetUpSource();
            await source.ProduceMessageAsync(wordCountMessage1);
            var batchCoordinator = client.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            await batchCoordinator.StartRecovery();
            int count = await source.GetStateInReverseLog(new StreamMessage(wordCountMessage1.Key, "me"));
            Assert.AreEqual(-2, count);
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
            return Task.CompletedTask;
        }

    }
}
