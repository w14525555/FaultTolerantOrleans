﻿using System;
using System.Threading.Tasks;
using GrainInterfaces;
using GrainInterfaces.Model;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Utils;

namespace OrleansClient
{
    public class StatefulStreamObserver : IAsyncObserver<StreamMessage>
    {
        private ILogger logger;
        private IStatefulOperator statefulOperator;
        private IBatchTracker tracker;

        public StatefulStreamObserver(ILogger logger)
        {
            this.logger = logger;
        }

        public StatefulStreamObserver(ILogger logger, IStatefulOperator consumer)
        {
            this.statefulOperator = consumer;
            this.logger = logger;
        }

        public StatefulStreamObserver(ILogger logger, IStatefulOperator consumer, IBatchTracker tracker)
        {
            this.statefulOperator = consumer;
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
            this.logger.LogInformation($"=={item.Created}==         {item.Key} said: {item.Value}");
            //When a consumer receive messages from stream,
            //the consumer needs to consume to the message
            //and its state may change
            //Besides, when they receive messages,
            //They should tell the tracker the message has been 
            //processed. 
            PrettyConsole.Line("Receice");

            if (item.Key == Constants.Barrier_Key)
            {
                TellTrackMessageSent(item);
            }
            else if (item.Key == Constants.Commit_Key)
            {
                //Commit Here
                PrettyConsole.Line("Commit and Update Logs");
                if (statefulOperator != null)
                {
                    //Update the reverse log and incremental log. 
                    ClearReverseLog();
                    PrettyConsole.Line("Clear Reverse Log");
                    UpdateIncrementalLog();
                    PrettyConsole.Line("Update Incremental Log");
                }
            }
            else
            {
                if (statefulOperator != null)
                {
                    statefulOperator.ConsumeMessage(item);
                }
            }
            return Task.CompletedTask;
        }

        private Task TellTrackMessageSent(StreamMessage item)
        {
            if (tracker != null)
            {
                tracker.CompleteTracking(item);
                PrettyConsole.Line("Complete one msg");
            }
            return Task.CompletedTask;
        }

        public Task SetTracker(IBatchTracker batchTracker)
        {
            this.tracker = batchTracker;
            return Task.CompletedTask;
        }

        private Task ClearReverseLog()
        {
            statefulOperator.ClearReverseLog();
            return Task.CompletedTask;
        }

        private Task UpdateIncrementalLog()
        {
            statefulOperator.UpdateIncrementalLog();
            return Task.CompletedTask;
        }
    } 
}