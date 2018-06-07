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
        private IStreamSource room;
        private IStatefulOperator statefulOperator;
        private StatefulStreamObserver statefulStreamObserver;
        private static string joinedChannel = "general";
        private static string userName = "You Wu";
        private const string NOT_EXIST = "Not Exist";
        private const string INITIAL_KEY = "initialKey";
        private const string INITIAL_VALUE = "initialValue";
        private static string[] members;
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
            Assert.AreEqual(1, 1);
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
            statefulStreamObserver = new StatefulStreamObserver(mockLogger.Object);
            await stream.SubscribeAsync(statefulStreamObserver);
            members = await room.GetMembers();
            return Task.CompletedTask;
        }

    }
}
