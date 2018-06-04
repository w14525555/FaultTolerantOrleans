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
        private IClusterClient statefulClient;
        private IClusterClient statelessClient;
        private IStreamSource room;
        private IStatefulOperator statefulOperator;
        private IStatelessOperator statelessConsumer;
        private IBatchTracker batchTracker;
        private StatefulStreamObserver statefulStreamObserver;
        private StatelessStreamObserver statelessStreamObserver;
        private static string joinedChannel = "general";
        private static string userName = "You Wu";
        private static string userName2 = "Wu You";
        private static string NOT_EXIST = "Not Exist";
        private static string[] members;
        private static StreamMessage msg1 = new StreamMessage("key1", "value1");
        private static StreamMessage msg2 = new StreamMessage("key1", "value2");
        private static StreamMessage msg3 = new StreamMessage("initialKey", "newValue");
        private static StreamMessage msg4 = new StreamMessage("initialKey", "delete");
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
            Assert.AreEqual(statefulClient.IsInitialized, true);
        }

        [TestMethod]
        public async Task TestJoinRoom()
        {
            await SetUpSource();
            Assert.AreEqual(members.Length, 1);
        }

        // Test For State Management 

        [TestMethod]
        public async Task TestClientInsertState()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            string insertedState = await statefulOperator.GetState(msg1.Key);
            Assert.AreEqual(msg1.Value, insertedState);
        }

        [TestMethod]
        public async Task TestClientUpdateState()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            msg2.operation = Operation.Update;
            await room.Message(msg2);
            string updatedState = await statefulOperator.GetState(msg1.Key);
            Assert.AreEqual(msg2.Value, updatedState);
        }

        [TestMethod]
        public async Task TestClientDeleteState()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            msg2.operation = Operation.Delete;
            await room.Message(msg2);
            string deletedKeyState = await statefulOperator.GetState(msg1.Key);
            Assert.AreEqual(NOT_EXIST, deletedKeyState);
        }

        // Reverse Log Tests

        [TestMethod]
        public async Task TestReverseLogClearAfterCommit()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            await statefulOperator.ClearReverseLog();
            string stateAfterCommit = await statefulOperator.GetStateInReverseLog(msg1.Key);
            Assert.AreEqual(NOT_EXIST, stateAfterCommit);
        }

        [TestMethod]
        public async Task TestReverseLogOnUpdateState()
        {
            await SetUpSource();
            msg3.operation = Operation.Update;
            await room.Message(msg3);
            string previousState = await statefulOperator.GetStateInReverseLog(msg3.Key);
            Assert.AreEqual("initialValue", previousState);
        }

        [TestMethod]
        public async Task TestReverseLogDeleteState()
        {
            await SetUpSource();
            msg4.operation = Operation.Delete;
            await room.Message(msg4);
            string previousState = await statefulOperator.GetStateInReverseLog(msg3.Key);
            Assert.AreEqual("initialValue", previousState);
        }

        [TestMethod]
        public async Task TestReverseLogInsertState()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            string insertedStateInReverseLog = await statefulOperator.GetStateInReverseLog(msg1.Key);
            Assert.AreEqual(null, insertedStateInReverseLog);
        }

        // Incremental Log Tests

        [TestMethod]
        public async Task TestIncrementalLogInsertState()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            string insertedStateInIncrementalLog = await statefulOperator.GetStateInIncrementalLog(msg1.Key);
            Assert.AreEqual(msg1.Value, insertedStateInIncrementalLog);
        }

        [TestMethod]
        public async Task TestIncrementalLogUpdateState()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            msg2.operation = Operation.Update;
            await statefulOperator.UpdateOperation(msg2);
            string updatedState = await statefulOperator.GetStateInIncrementalLog(msg1.Key);
            Assert.AreEqual(msg2.Value, updatedState);
        }

        [TestMethod]
        public async Task TestIncrementalLogDeleteState()
        {
            await SetUpSource();
            msg1.operation = Operation.Insert;
            await room.Message(msg1);
            msg2.operation = Operation.Delete;
            await room.Message(msg2);
            string stateAfterDelete = await statefulOperator.GetStateInIncrementalLog(msg1.Key);
            Assert.AreEqual(null, stateAfterDelete);
        }

        // Batch Processing Tests

        [TestMethod]
        public async Task TestEmtyBatchSentThenTheBatchIsReadForCommit()
        {
            await SetUpSource();
            await SetUpTracker();
            barrierMsg.BatchID = 0;
            await room.Message(barrierMsg);
            bool isCurrentBatchCompleted = await batchTracker.IsReadForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

        [TestMethod]
        public async Task TestWhenAllMessageSentTheBatchIsReadForCommit()
        {
            await SetUpSource();
            await SetUpTracker();
            await room.Message(msg1);
            await room.Message(msg2);
            barrierMsg.BatchID = 0;
            await room.Message(barrierMsg);
            bool isCurrentBatchCompleted = await batchTracker.IsReadForCommit(barrierMsg.BatchID);
            Assert.AreEqual(true, isCurrentBatchCompleted);
        }

        // multiple client tests

        [TestMethod]
        public async Task TestMultipleClients()
        {
            await StartStatelessClient();
            await SetUpSourceWithMultipleClients();
            Assert.AreEqual(members.Length, 2);
        }

        [TestMethod]
        public async Task TestEmptyBatchSentWithMultipleClientsThenTheBatchIsReadForCommit()
        {
            await StartStatelessClient();
            await SetUpSourceWithMultipleClients();
            await SetUpTracker();
            barrierMsg.BatchID = 0;
            await room.Message(barrierMsg);
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
            statefulClient = await GetClient();
            await statefulClient.Connect();
            
            return Task.CompletedTask;
        }

        private async Task<Task> StartStatelessClient()
        {
            statelessClient = await GetClient();
            await statelessClient.Connect();

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
            statefulClient.Close().Wait();
            if (statelessClient != null)
            {
                statelessClient.Close().Wait();
            }
            return Task.CompletedTask;
        }

        private async Task<Task> SetUpSource()
        {
            room = statefulClient.GetGrain<IStreamSource>(joinedChannel);
            var streamId = await room.Join(userName);
            var stream = statefulClient.GetStreamProvider(Constants.ChatRoomStreamProvider)
                .GetStream<StreamMessage>(streamId, Constants.CharRoomStreamNameSpace);
            //subscribe to the stream to receiver furthur messages sent to the chatroom
            statefulOperator = statefulClient.GetGrain<IStatefulOperator>("Consumer");
            Mock<ILogger> mockLogger = new Mock<ILogger>();
            statefulStreamObserver = new StatefulStreamObserver(mockLogger.Object, statefulOperator);
            await stream.SubscribeAsync(statefulStreamObserver);
            members = await room.GetMembers();
            return Task.CompletedTask;
        }

        private async Task<Task> SetUpSourceWithMultipleClients()
        {
            await SetUpSource();
            var streamId = await room.Join(userName2);
            var stream = statelessClient.GetStreamProvider(Constants.ChatRoomStreamProvider)
                .GetStream<StreamMessage>(streamId, Constants.CharRoomStreamNameSpace);
            //subscribe to the stream to receiver furthur messages sent to the chatroom
            statelessConsumer = statelessClient.GetGrain<IStatelessOperator>("Consumer2");
            Mock<ILogger> mockLogger = new Mock<ILogger>();
            statelessStreamObserver = new StatelessStreamObserver(mockLogger.Object, statelessConsumer);
            await stream.SubscribeAsync(statelessStreamObserver);
            members = await room.GetMembers();
            return Task.CompletedTask;
        }

        private async Task<Task> SetUpTracker()
        {
            batchTracker = await room.GetBatchTracker();
            await statefulStreamObserver.SetTracker(batchTracker);
            if (statelessStreamObserver != null)
            {
                await statelessStreamObserver.SetTracker(batchTracker);
            }
            return Task.CompletedTask;
        }
    }
}
