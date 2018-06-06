using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Streams;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

namespace OrleansClient
{
    public class StatefulStreamObserver : IAsyncObserver<StreamMessage>
    {
        private ILogger logger;
        private IStatefulOperator statefulOperator;
        private IBatchTracker tracker;
        private IBatchCoordinator batchCoordinator;
        private IClusterClient client;
        private bool isGrainFailed = false;
        private OperatorSettings operatorSettings;
        private int currentBatchID = -1;

        private List<StreamMessage> messagesBuffer = new List<StreamMessage>();

        public StatefulStreamObserver(ILogger logger)
        {
            this.logger = logger;
        } 

        public StatefulStreamObserver(ILogger logger, IStatefulOperator statefulOperator)
        {
            this.statefulOperator = statefulOperator;
            this.logger = logger;
        }

        public StatefulStreamObserver(ILogger logger, IStatefulOperator statefulOperator, IBatchTracker tracker)
        {
            this.statefulOperator = statefulOperator;
            this.logger = logger;
            this.tracker = tracker;
        }

        public async Task<Task> LoadSettings()
        {
            if (statefulOperator != null)
            {
                operatorSettings = await statefulOperator.GetSettings();
            }
            return Task.CompletedTask;
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
            //When a consumer receive a message from stream,
            //It firstly checks which batch this belongs 
            //the consumer needs to consume to the message
            //and its state may change
            //Besides, when they receive messages,
            //They should tell the tracker the message has been 
            //processed. 
            if (currentBatchID == -1)
            {
                currentBatchID = msg.BatchID;
                ProcessMessages(msg);
            }
            else
            {
                if (currentBatchID == msg.BatchID)
                {
                    ProcessMessages(msg);
                }
                else
                {
                    SaveMessageToBuffer(msg);
                }
            }
            return Task.CompletedTask;
        }

        private Task ProcessMessages(StreamMessage msg)
        {
            PrettyConsole.Line("Stateful Operator Receice A Message");

            if (msg.Key == Constants.Barrier_Key)
            {
                TellTrackMessageSent(msg);
            }
            else if (msg.Key == Constants.Commit_Key)
            {
                //Commit Here
                PrettyConsole.Line("Commit and Update Logs");
                if (statefulOperator != null)
                {
                    //Update the reverse log and incremental log.
                    try
                    {
                        ClearReverseLog();
                        PrettyConsole.Line("Clear Reverse Log");
                        UpdateIncrementalLog();
                        PrettyConsole.Line("Update Incremental Log");
                        currentBatchID++;
                        CheckIfBufferHasNextBatchMessages();
                    }
                    catch(Exception e)
                    {
                        PrettyConsole.Line("Catch Exception: " + e);
                        StartRecovery();
                    }
                }
            }
            else
            {
                if (statefulOperator != null)
                {
                    try
                    {
                        statefulOperator.ConsumeMessage(msg);
                    }
                    catch (Exception e)
                    {
                        PrettyConsole.Line("Catch Exception: " + e);
                        StartRecovery();
                    }
                }
            }
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

        private Task SaveMessageToBuffer(StreamMessage msg)
        {
            messagesBuffer.Add(msg);
            return Task.CompletedTask;
        }

        public Task SetTracker(IBatchTracker batchTracker)
        {
            tracker = batchTracker;
            return Task.CompletedTask;
        }

        public Task SetCoordinator(IBatchCoordinator batchCoordinator)
        {
            this.batchCoordinator = batchCoordinator;
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

        private Task CheckIfBufferHasNextBatchMessages()
        {
            foreach (StreamMessage msg in messagesBuffer)
            {
                if (msg.BatchID == currentBatchID)
                {
                    ProcessMessages(msg);
                }
            }
            return Task.CompletedTask;
        }

        //TODO
        private Task StartRecovery()
        {
            //The failed grain will not revert from reverse log
            isGrainFailed = true;
            //Should tell batch manager start recovery
            //Broadcast message
            //Start A new Stateful Operator Grain
            StartANewGrain();
            batchCoordinator.StartRecovery();
            return Task.CompletedTask;
        }

        private Task StartANewGrain()
        {
            //Start a New grain with different id
            statefulOperator = client.GetGrain<IStatefulOperator>("NewOperator");
            if (operatorSettings != null)
            {
                //Load the settings of old grain
                statefulOperator.LoadSettings(operatorSettings);
            }
            return Task.CompletedTask;
        }

        public Task SetClient(IClusterClient client)
        {
            this.client = client;
            return Task.CompletedTask;
        }
    } 
}
