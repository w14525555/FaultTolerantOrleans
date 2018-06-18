using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Utils;
using SystemInterfaces;
using SystemInterfaces.Model;
using System;
using Orleans.Streams;
using System.Linq;
using System.Diagnostics;

namespace SystemImplementation
{
    class StatelessStreamOperator : Grain, IStatelessOperator
    {
        //The StatelessConsumer does not have state.
        private List<IStatefulOperator> statefulOperators;
        private IBatchTracker batchTracker;
        private OperatorSettings operatorSettings = new OperatorSettings();

        public override Task OnActivateAsync()
        {
            batchTracker = GrainFactory.GetGrain<IBatchTracker>(Constants.Tracker);
            operatorSettings.operatorType = OperatorType.Stateless;
            return base.OnActivateAsync();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> InitRandomOperators()
        {
            statefulOperators = new List<IStatefulOperator>();
            IStatefulOperator operatorOne = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
            IStatefulOperator operatorTwo = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
            statefulOperators.Add(operatorOne);
            operatorOne.IncrementNumberOfUpStreamOperator();
            statefulOperators.Add(operatorTwo);
            operatorTwo.IncrementNumberOfUpStreamOperator();
            operatorSettings.AddOpratorToDict(operatorOne.GetPrimaryKey(), await operatorOne.GetOperatorSettings());
            operatorSettings.AddOpratorToDict(operatorTwo.GetPrimaryKey(), await operatorTwo.GetOperatorSettings());
            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> InitCustomerOperators(List<Guid> guidList)
        {
            statefulOperators = new List<IStatefulOperator>();
            foreach (var item in guidList)
            {
                IStatefulOperator op = GrainFactory.GetGrain<IStatefulOperator>(item);
                statefulOperators.Add(op);
                op.IncrementNumberOfUpStreamOperator();
                operatorSettings.AddOpratorToDict(op.GetPrimaryKey(), await op.GetOperatorSettings());

            }
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
                int index = SystemImplementation.PartitionFunction.PartitionStatefulByKey(msg.Key, statefulOperators.Count);
                IStatefulOperator statefulOperator = statefulOperators.ElementAt(index);
                StreamMessage newMessage = new StreamMessage(word, null);
                newMessage.BatchID = msg.BatchID;
                //await statefulOperator.ExecuteMessage(newMessage, stream);
                await ExecuteMessagesByDownStreamOperators(newMessage, stream, statefulOperator, index);
            }
            return Task.CompletedTask;
        }

        private async Task<Task> ExecuteMessagesByDownStreamOperators(StreamMessage msg, IAsyncStream<StreamMessage> stream, IStatefulOperator statefulOperator, int index)
        {
            try
            {
                await statefulOperator.ExecuteMessage(msg, stream);
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                PrettyConsole.Line("Get Exception : " + e + "; Start Receovry");
                //1. Restart a new grain
                IStatefulOperator newOperator = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
                //2. Rollback the state
                //a. Find the operator settings
                var item = operatorSettings.GetOperatorDict().ElementAt(index);
                //b. Load the setting 
                await newOperator.LoadSettings(item.Value);
                //c. mark as failed, so when it receive recovery message it will 
                //revert states by incremental log
                await newOperator.MarkOperatorAsFailed();
                //3. Remove the failed from the topology
                statefulOperators.RemoveAt(index);
                operatorSettings.RemoveOperatorFromDict(item.Key);
                //4. Add the new grain to topology
                statefulOperators.Add(newOperator);
                operatorSettings.AddOpratorToDict(newOperator.GetPrimaryKey(), await newOperator.GetOperatorSettings());
                //5. Start Recovery
                var batchCoordinator = GrainFactory.GetGrain<IBatchCoordinator>(Constants.Coordinator);
                await batchCoordinator.StartRecovery();
                return Task.CompletedTask;
            }
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
                PrettyConsole.Line("Tracking with stateless with " + statefulOperators.Count);
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

        private async Task<Task> BroadcastSpecialMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            int index = 0;
            foreach (IStatefulOperator item in statefulOperators)
            {
                await ExecuteMessagesByDownStreamOperators(msg, stream, item, index);
                index++;
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

        public Task<Task> InitCustomOperators(List<Guid> guidList)
        {
            throw new NotImplementedException();
        }

        public Task<TopologyUnit> GetTopologyUnit()
        {
            throw new NotImplementedException();
        }
    }
}
