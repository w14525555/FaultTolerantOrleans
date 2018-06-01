using System;
using System.Threading.Tasks;
using GrainInterfaces;
using GrainInterfaces.Model;
using Orleans.Streams;

namespace SystemInterfaces.Model
{
    class StatelessStreamObserver : IAsyncObserver<StreamMessage>
    {
        private IStatelessOperator statelessStreamOperator;
        private IBatchTracker tracker;

        public Task OnCompletedAsync()
        {
            throw new NotImplementedException();
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }

        public Task OnNextAsync(StreamMessage item, StreamSequenceToken token = null)
        {
            throw new NotImplementedException();
        }
    }
}
