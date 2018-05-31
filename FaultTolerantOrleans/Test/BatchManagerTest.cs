using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Hosting;
using Orleans.Configuration;
using Utils;
using System.Net;
using GrainImplementation;
using GrainInterfaces;
using Orleans;
using GrainInterfaces.Model;
using OrleansClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Tasks;
using Moq;

namespace Test
{
    [TestClass]
    public class BatchManagerTest
    {
        private ISiloHost silo;
        private IClusterClient client;
        private IStreamSource room;
        private IOperator consumer;
        private IBatchTracker batchTracker;
        private StreamObserver streamObserver;
        private static string joinedChannel = "general";
        private static string userName = "You Wu";
        private static string NOT_EXIST = "Not Exist";
        private static string[] members;
        private static StreamMessage msg1 = new StreamMessage("initial", "message 1");
        private static StreamMessage msg2 = new StreamMessage("second", "message 2");
        private static StreamMessage msg3 = new StreamMessage("initial", "message 3");
        private static StreamMessage msg4 = new StreamMessage("initial", "message 4");
        private static StreamMessage barrierMsg = new StreamMessage(Constants.Barrier_Key, Constants.System_Value);
        private static StreamMessage commitMsg = new StreamMessage(Constants.Commit_Key, Constants.System_Value);


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

        [TestMethod]
        public void TestSiloAndClinetInitAsync()
        {
            Assert.AreEqual(client.IsInitialized, true);
        }

        [TestMethod]
        public async Task TestJoinRoom()
        {
            await SetUpRoom();
            Assert.AreEqual(members.Length, 1);
        }

        // Test For State Management 

        [TestMethod]
        public async Task TestClientInitialState()
        {
            await SetUpRoom();
            string initialState = await consumer.GetState("initial");
            Assert.AreEqual("hello", initialState);
        }

        [TestMethod]
        public async Task TestClientUpdateState()
        {
            await SetUpRoom();
            msg1.operation = Operation.Update;
            await room.Message(msg1);
            string updatedState = await consumer.GetState("initial");
            Assert.AreEqual(msg1.Value, updatedState);
        }

        [TestMethod]
        public async Task TestClientInsertState()
        {
            await SetUpRoom();
            msg2.operation = Operation.Insert;
            await room.Message(msg2);
            string insertedState = await consumer.GetState("second");
            Assert.AreEqual(msg2.Value, insertedState);
        }

        [TestMethod]
        public async Task TestClientDeleteState()
        {
            await SetUpRoom();
            msg1.operation = Operation.Delete;
            await room.Message(msg1);
            string deletedState = await consumer.GetState("initial");
            Assert.AreEqual(NOT_EXIST, deletedState);
        }

        // Reverse Log Tests

        [TestMethod]
        public async Task TestReverseLogInitialState()
        {
            await SetUpRoom();
            string initialState = await consumer.GetStateInReverseLog("initial");
            Assert.AreEqual(null, initialState);
        }

        [TestMethod]
        public async Task TestReverseLogClearAfterCommit()
        {
            await SetUpRoom();
            await consumer.ClearReverseLog();
            string stateAfterCommit = await consumer.GetStateInReverseLog("initial");
            Assert.AreEqual(NOT_EXIST, stateAfterCommit);
        }

        [TestMethod]
        public async Task TestReverseLogUpdateState()
        {
            await SetUpRoom();
            msg1.operation = Operation.Update;
            await room.Message(msg1);
            string updatedState = await consumer.GetStateInReverseLog("initial");
            Assert.AreEqual("hello", updatedState);
        }

        [TestMethod]
        public async Task TestReverseLogDeleteState()
        {
            await SetUpRoom();
            msg1.operation = Operation.Delete;
            await room.Message(msg1);
            string previousState = await consumer.GetStateInReverseLog("initial");
            Assert.AreEqual("hello", previousState);
        }

        [TestMethod]
        public async Task TestReverseLogInsertState()
        {
            await SetUpRoom();
            msg2.operation = Operation.Insert;
            await room.Message(msg2);
            string insertedStateInReverseLog = await consumer.GetStateInReverseLog("second");
            Assert.AreEqual(null, insertedStateInReverseLog);
        }

        // Incremental Log Tests

        [TestMethod]
        public async Task TestIncrementalLogInitialState()
        {
            await SetUpRoom();
            string initialState = await consumer.GetStateInIncrementalLog("initial");
            Assert.AreEqual("hello", initialState);
        }

        [TestMethod]
        public async Task TestIncrementalLogClearAfterCommit()
        {
            await SetUpRoom();
            await consumer.UpdateIncrementalLog();
            string stateAfterCommit = await consumer.GetStateInIncrementalLog("initial");
            Assert.AreEqual(NOT_EXIST, stateAfterCommit);
        }

        [TestMethod]
        public async Task TestIncrementalLogUpdateState()
        {
            await SetUpRoom();
            msg1.operation = Operation.Update;
            await consumer.UpdateOperation(msg1);
            string updatedState = await consumer.GetStateInIncrementalLog("initial");
            Assert.AreEqual(msg1.Value, updatedState);
        }

        [TestMethod]
        public async Task TestIncrementalLogDeleteState()
        {
            await SetUpRoom();
            msg1.operation = Operation.Delete;
            await room.Message(msg1);
            string stateAfterDelete = await consumer.GetStateInIncrementalLog("initial");
            Assert.AreEqual(null, stateAfterDelete);
        }

        [TestMethod]
        public async Task TestIncrementalLogInsertState()
        {
            await SetUpRoom();
            msg2.operation = Operation.Insert;
            await room.Message(msg2);
            string insertedStateInIncrementalLog = await consumer.GetStateInIncrementalLog("second");
            Assert.AreEqual(msg2.Value, insertedStateInIncrementalLog);
        }

        // Batch Processing Tests

        [TestMethod]
        public async Task TestBatchIDStartsWithZero()
        {
            await SetUpRoom();
            await room.Message(msg1);
            Assert.AreEqual(0, msg1.BatchID);
        }

        [TestMethod]
        public async Task TestEmtyBatchSentThenTheBatchIsReadForCommit()
        {
            await SetUpRoom();
            await SetUpTracker();
            barrierMsg.BatchID = 0;
            await room.Message(barrierMsg);
            bool isCurrentBatchCompleted = await batchTracker.IsReadForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

        [TestMethod]
        public async Task TestWhenAllMessageSentTheBatchIsReadForCommit()
        {
            await SetUpRoom();
            await SetUpTracker();
            await room.Message(msg1);
            await room.Message(msg2);
            barrierMsg.BatchID = 0;
            await room.Message(barrierMsg);
            bool isCurrentBatchCompleted = await batchTracker.IsReadForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

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
            client = new ClientBuilder().Configure<ClusterOptions>(options =>
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
            await client.Connect();
            
            return Task.CompletedTask;
        }

        private Task StopClient()
        {
            client.Close().Wait();
            return Task.CompletedTask;
        }

        private async Task<Task> SetUpRoom()
        {
            room = client.GetGrain<IStreamSource>(joinedChannel);
            var streamId = await room.Join(userName);
            var stream = client.GetStreamProvider(Constants.ChatRoomStreamProvider)
                .GetStream<StreamMessage>(streamId, Constants.CharRoomStreamNameSpace);
            //subscribe to the stream to receiver furthur messages sent to the chatroom
            consumer = client.GetGrain<IOperator>("Consumer");
            Mock<ILogger> mockLogger = new Mock<ILogger>();
            streamObserver = new StreamObserver(mockLogger.Object, consumer);
            await stream.SubscribeAsync(streamObserver);
            members = await room.GetMembers();
            return Task.CompletedTask;
        }

        private async Task<Task> SetUpTracker()
        {
            batchTracker = await room.GetBatchTracker();
            await streamObserver.SetTracker(batchTracker);
            return Task.CompletedTask;
        }
    }
}
