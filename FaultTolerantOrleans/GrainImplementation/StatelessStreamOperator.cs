using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Utils;
using SystemInterfaces;
using SystemInterfaces.Model;
using System;
using Orleans.Streams;

namespace SystemImplementation
{
    class StatelessStreamOperator : Grain, IStatelessOperator
    {
        //The StatelessConsumer does not have state.
        private HashSet<IStatefulOperator> statefulOperators;
        private IBatchTracker batchTracker;

        public override Task OnActivateAsync()
        {
            InitOperators();
            return base.OnActivateAsync();
        }

        private Task InitOperators()
        {
            statefulOperators = new HashSet<IStatefulOperator>();
            IStatefulOperator operatorOne = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
            IStatefulOperator operatorTwo = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
            statefulOperators.Add(operatorOne);
            statefulOperators.Add(operatorTwo);
            return Task.CompletedTask;
        }

        public async Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            //At first split text into words
            if (msg.Key != Constants.System_Key)
            {
                await SplitWordsAndExcuteMsg(msg, stream);
            }
            else
            {
                await ProcessSpecialMessageAsync(msg, stream);
            }
            return Task.CompletedTask;
        }

        private async Task<Task> SplitWordsAndExcuteMsg(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            List<string> words = Utils.Functions.SpiltIntoWords(msg.Value);
            //Then find a operator
            foreach (string word in words)
            {
                IStatefulOperator statefulOperator = await SystemImplementation.PartitionFunction.PartitionStatefulByKey(msg.Key, statefulOperators);
                StreamMessage newMessage = new StreamMessage(word, null);
                newMessage.BatchID = msg.BatchID;
                await statefulOperator.ExecuteMessage(newMessage, stream);
            }
            return Task.CompletedTask;
        }

        private async Task<Task> ProcessSpecialMessageAsync(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            BarrierOrCommitMsgTrackingInfo info = new BarrierOrCommitMsgTrackingInfo(msg.barrierOrCommitInfo.GetID(), msg.barrierOrCommitInfo.numberOfClientSent);
            info.BatchID = msg.BatchID;
            if (msg.Value == Constants.Barrier_Value)
            {
                await HandleBarrierMessages(msg);
                await batchTracker.CompleteOneOperatorBarrier(info);
            }
            //The stateless operator does not have state
            //so it just broadcast messages. 
            else if (msg.Value == Constants.Commit_Value)
            {
                await HandleCommitMessages(msg);
                await batchTracker.CompleteOneOperatorCommit(info);
            }
            else if (msg.Value == Constants.Recovery_Value)
            {
                await HandleRecoveryMessages(msg);
                await batchTracker.CompleteOneOperatorRecovery(info);
            }
            await BroadcastSpecialMessage(msg, stream);
            return Task.CompletedTask;
        }

        private Task HandleBarrierMessages(StreamMessage msg)
        {
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), statefulOperators.Count);
            msg.barrierOrCommitInfo.BatchID = msg.BatchID;
            if (batchTracker != null)
            {
                batchTracker.TrackingBarrierMessages(msg);
            }
            else
            {
                throw new NullReferenceException();
            }
            return Task.CompletedTask;
        }

        private Task HandleCommitMessages(StreamMessage msg)
        {
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), statefulOperators.Count);
            msg.barrierOrCommitInfo.BatchID = msg.BatchID;
            if (batchTracker != null)
            {
                batchTracker.TrackingCommitMessages(msg);
            }
            else
            {
                throw new NullReferenceException();
            }
            return Task.CompletedTask;
        }

        private Task HandleRecoveryMessages(StreamMessage msg)
        {
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), statefulOperators.Count);
            msg.barrierOrCommitInfo.BatchID = msg.BatchID;
            if (batchTracker != null)
            {
                batchTracker.TrackingRecoveryMessages(msg);
            }
            else
            {
                throw new NullReferenceException();
            }
            return Task.CompletedTask;
        }

        private Task BroadcastSpecialMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            foreach (IStatefulOperator item in statefulOperators)
            {
                item.ExecuteMessage(msg, stream);
            }
            return Task.CompletedTask;
        }

        public Task SetBatchTracker(IBatchTracker batchTracker)
        {
            this.batchTracker = batchTracker;
            foreach (IStatefulOperator statefulOp in statefulOperators)
            {
                statefulOp.SetTracker(batchTracker);
            }
            return Task.CompletedTask;
        }

        public async Task<int> GetState(string word)
        {
            foreach(var op in statefulOperators)
            {
                var count = await op.GetState(word);
                if (count != -1)
                {
                    return await Task.FromResult(count);
                }
            }
            return await Task.FromResult(-1);
        }

        public async Task<int> GetStateInReverseLog(string word)
        {
            foreach (var op in statefulOperators)
            {
                var count = await op.GetStateInReverseLog(word);
                if (count != -1)
                {
                    return await Task.FromResult(count);
                }
            }
            return await Task.FromResult(-1);
        }

        public async Task<int> GetStateInIncrementalLog(string word)
        {
            foreach (var op in statefulOperators)
            {
                var count = await op.GetStateInIncrementalLog(word);
                if (count != -1)
                {
                    return await Task.FromResult(count);
                }
            }
            return await Task.FromResult(-1);
        }
    }
}
