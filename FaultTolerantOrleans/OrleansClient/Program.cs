using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Utils;
using SystemInterfaces;
using SystemInterfaces.Model;

namespace OrleansClient
{
    class Program
    {
        //To make this sample simple
        //In this sample, one client can only join one channel, hence we have a static variable of one channel name.
        //client can send messages to the channel , and receive messages sent to the channel/stream from other clients. 
        private static Guid joinedChannel = Guid.NewGuid();
        private static string userName = "UserWithNoName";
        public static void Main(string[] args)
        {
            var clientInstance = InitializeClient(args).GetAwaiter().GetResult();

            PrettyConsole.Line("==== CLIENT: Initialized ====", ConsoleColor.Cyan);
            PrettyConsole.Line("CLIENT: Write commands:", ConsoleColor.Cyan);


            Menu(clientInstance).GetAwaiter().GetResult();

            PrettyConsole.Line("==== CLIENT: Shutting down ====", ConsoleColor.DarkRed);
        }

        private static async Task<IClusterClient> InitializeClient(string[] args)
        {
            int initializeCounter = 0;

            var initSucceed = false;
            while (!initSucceed)
            {
                try
                {
                    var client = new ClientBuilder().Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = Constants.ClusterId;
                            options.ServiceId = Constants.ServiceId;
                        })
                        .UseLocalhostClustering()
                        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IStreamSource).Assembly).WithReferences())
                        .ConfigureLogging(logging => logging.AddConsole())
                        //Depends on your application requirements, you can configure your client with other stream providers, which can provide other features, 
                        //such as persistence or recoverability. For more information, please see http://dotnet.github.io/orleans/Documentation/Orleans-Streams/Stream-Providers.html
                        .AddSimpleMessageStreamProvider(Constants.FaultTolerantStreamProvider)
                        .Build();
                    await client.Connect();
                    initSucceed = client.IsInitialized;

                    if (initSucceed)
                    {
                        return client;
                    }
                }
                catch (Exception exc)
                {
                    PrettyConsole.Line(exc.Message, ConsoleColor.Cyan);
                    initSucceed = false;
                }

                if (initializeCounter++ > 10)
                {
                    return null;
                }

                PrettyConsole.Line("Client Init Failed. Sleeping 5s...", ConsoleColor.Red);
                Thread.Sleep(TimeSpan.FromSeconds(5));
            }

            return null;
        }

        private static void PrintHints()
        {
            var menuColor = ConsoleColor.Magenta;
            PrettyConsole.Line("Type '/start to start with default topology", menuColor);
            PrettyConsole.Line("Type '<any text>' to send a message", menuColor);
        }

        private static async Task Menu(IClusterClient client)
        {
            string input;
            PrintHints();
            do
            {
                input = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(input)) continue;

                if (input.StartsWith("/start"))
                {
                    await StartDefaultTopology(client);
                }
                else if (!input.StartsWith("/exit"))
                {
                    await SendMessage(client, input);
                }
            } while (input != "/exit");
        }

        private static async Task SendMessage(IClusterClient client, string messageText)
        {
            var room = client.GetGrain<IStreamSource>(joinedChannel);
            await room.Message(new StreamMessage(userName, messageText));
        }

        private static async Task StartDefaultTopology(IClusterClient client)
        {
            var source = client.GetGrain<IStreamSource>(joinedChannel);
            await source.InitDeaultOperators();
            //var room2 = client.GetGrain<IStreamSource>("new");
            var streamId = await source.Join(userName);
            //var streamId2 = await room2.Join(userName);
            var stream = client.GetStreamProvider(Constants.FaultTolerantStreamProvider)
                .GetStream<StreamMessage>(streamId, Constants.FaultTolerantStreamNameSpace);
            //subscribe to the stream to receiver furthur messages sent to the chatroom
            StatefulStreamObserver observer = new StatefulStreamObserver(client.ServiceProvider.GetService<ILoggerFactory>()
                .CreateLogger($"{joinedChannel} channel"));
            await stream.SubscribeAsync(observer);
        }

    }
}
