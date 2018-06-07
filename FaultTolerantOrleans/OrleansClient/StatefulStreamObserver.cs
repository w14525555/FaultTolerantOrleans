using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using SystemInterfaces.Model;
using Utils;

namespace OrleansClient
{
    public class StatefulStreamObserver : IAsyncObserver<StreamMessage>
    {
        private ILogger logger;

        public StatefulStreamObserver(ILogger logger)
        {
            this.logger = logger;
        } 

        public Task OnCompletedAsync()
        {
            this.logger.LogInformation("Message stream received stream completed event");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            this.logger.LogInformation($"Experiencing message delivery failure, ex :{ex}");
            return Task.CompletedTask;
        }

        public Task OnNextAsync(StreamMessage msg, StreamSequenceToken token = null)
        {
            PrettyConsole.Line("Receive Message, Key: " + msg.Key + " Value: " + msg.Value);
            return Task.CompletedTask;
        }

    } 
}
