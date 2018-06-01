using System;
using System.Threading.Tasks;
using GrainInterfaces;
using GrainInterfaces.Model;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace SystemInterfaces.Model
{
    class StatelessStreamObserver : IAsyncObserver<StreamMessage>
    {
        private IStatelessOperator statelessStreamOperator;
        private IBatchTracker tracker;
        private ILogger logger;

        public StatelessStreamObserver(ILogger logger)
        {
            this.logger = logger;
        }

        public StatelessStreamObserver(ILogger logger, IStatelessOperator statelessStreamOperator)
        {
            this.statelessStreamOperator = statelessStreamOperator;
            this.logger = logger;
        }

        public StatelessStreamObserver(ILogger logger, IStatelessOperator statelessStreamOperator, IBatchTracker tracker)
        {
            this.statelessStreamOperator = statelessStreamOperator;
            this.logger = logger;
            this.tracker = tracker;
        }

        public Task OnCompletedAsync()
        {
            this.logger.LogInformation("Chatroom message stream received stream completed event");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            this.logger.LogInformation($"Chatroom is experiencing message delivery failure, ex :{ex}");
            return Task.CompletedTask;
        }

        public Task OnNextAsync(StreamMessage item, StreamSequenceToken token = null)
        {
            throw new NotImplementedException();
        }
    }
}
