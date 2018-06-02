using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GrainInterfaces;
using GrainInterfaces.Model;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Utils;

namespace SystemInterfaces.Model
{
    class StatelessStreamObserver : IAsyncObserver<StreamMessage>
    {
        private IStatelessOperator statelessStreamOperator;
        private IBatchTracker tracker;
        private ILogger logger;
        private List<StreamMessage> messagesBuffer;
        private int currentBatchID = -1;

        public StatelessStreamObserver(ILogger logger, IStatelessOperator statelessStreamOperator, IBatchTracker tracker)
        {
            this.statelessStreamOperator = statelessStreamOperator;
            this.logger = logger;
            this.tracker = tracker;
            this.messagesBuffer = new List<StreamMessage>();
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

        public Task OnNextAsync(StreamMessage msg, StreamSequenceToken token = null)
        {
            if (currentBatchID == -1)
            {
                currentBatchID = msg.BatchID;
                FilterFunction();
            }
            else
            {
                if (msg.BatchID == currentBatchID)
                {
                    FilterFunction();
                }
                else
                {
                    SaveMessageToBuffer(msg);
                }
            }
            return Task.CompletedTask;
        }

        private Task FilterFunction()
        {
            PrettyConsole.Line("Filtered in stateless function");
            return Task.CompletedTask;
        }

        private Task SaveMessageToBuffer(StreamMessage msg)
        {
            messagesBuffer.Add(msg);
            return Task.CompletedTask;
        }
    }
}
