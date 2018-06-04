using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Utils;

namespace SystemInterfaces.Model
{
    public class StatelessStreamObserver : IAsyncObserver<StreamMessage>
    {
        private IStatelessOperator statelessStreamOperator;
        private IBatchTracker tracker;
        private ILogger logger;
        private List<StreamMessage> messagesBuffer;
        private int currentBatchID = -1;

        public StatelessStreamObserver(ILogger logger, IStatelessOperator statelessStreamOperator)
        {
            this.statelessStreamOperator = statelessStreamOperator;
            this.logger = logger;
            this.messagesBuffer = new List<StreamMessage>();
        }

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
                ProcessMessage(msg);
            }
            else
            {
                if (msg.BatchID == currentBatchID)
                {
                    ProcessMessage(msg);
                }
                else
                {
                    SaveMessageToBuffer(msg);
                }
            }
            return Task.CompletedTask;
        }

        private Task ProcessMessage(StreamMessage msg)
        {
            PrettyConsole.Line("Process a message in Stateless Operator");
            if (msg.Key == Constants.Barrier_Key)
            {
                TellTrackMessageSent(msg);
            }
            else if (msg.Key == Constants.Commit_Key)
            {
                currentBatchID++;
                CheckIfBufferHasNextBatchMessages();
            }
            else
            {
                OperationFunction(msg);
            }
            return Task.CompletedTask;
        }

        private Task OperationFunction(StreamMessage msg)
        {
            return Task.CompletedTask;
        }

        private Task SaveMessageToBuffer(StreamMessage msg)
        {
            messagesBuffer.Add(msg);
            return Task.CompletedTask;
        }

        private Task TellTrackMessageSent(StreamMessage item)
        {
            if (tracker != null)
            {
                tracker.CompleteTracking(item);
                PrettyConsole.Line("Complete one barrier");
            }
            return Task.CompletedTask;
        }

        private Task CheckIfBufferHasNextBatchMessages()
        {
            foreach (StreamMessage msg in messagesBuffer)
            {
                if (msg.BatchID == currentBatchID)
                {
                    ProcessMessage(msg);
                }
            }
            return Task.CompletedTask;
        }

        public Task SetTracker(IBatchTracker batchTracker)
        {
            this.tracker = batchTracker;
            return Task.CompletedTask;
        }
    }
}
