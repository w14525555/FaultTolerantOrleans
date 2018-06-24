using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Utils;
using SystemInterfaces;
using SystemInterfaces.Model;
using System;
using Orleans.Streams;
using System.Linq;

namespace SystemImplementation
{
    public abstract class StatelessStreamOperator : Grain, IStatelessOperator
    {
        //The StatelessConsumer does not have state.
            
        protected List<IStatefulOperator> statefulOperators = new List<IStatefulOperator>();
        protected IBatchTracker batchTracker;
        protected ITopology topologyManager;
        protected TopologyUnit topologyUnit;
        protected OperatorSettings operatorSettings = new OperatorSettings();
        private Dictionary<Guid, int> upStreamMessageCountMap = new Dictionary<Guid, int>();
        private Dictionary<Guid, int> downStreamMessageCountMap = new Dictionary<Guid, int>();

        public override Task OnActivateAsync()
        {
            batchTracker = GrainFactory.GetGrain<IBatchTracker>(Constants.Tracker);
            operatorSettings.operatorType = OperatorType.Stateless;
            topologyManager = GrainFactory.GetGrain<ITopology>(Constants.Topology_Manager);
            topologyUnit = new TopologyUnit(OperatorType.Stateless, this.GetPrimaryKey());
            topologyManager.AddUnit(topologyUnit);

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

            topologyManager.UpdateOperatorSettings(this.GetPrimaryKey(), operatorSettings);
            topologyManager.ConnectUnits(this.GetPrimaryKey(), operatorOne.GetPrimaryKey());
            topologyManager.ConnectUnits(this.GetPrimaryKey(), operatorTwo.GetPrimaryKey());

            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> AddCustomOperators(List<Guid> guidList)
        {
            foreach (var item in guidList)
            {
                IStatefulOperator op = GrainFactory.GetGrain<IStatefulOperator>(item);
                statefulOperators.Add(op);
                op.IncrementNumberOfUpStreamOperator();
                operatorSettings.AddOpratorToDict(op.GetPrimaryKey(), await op.GetOperatorSettings());
                topologyManager.ConnectUnits(topologyUnit.PrimaryKey, op.GetPrimaryKey());
            }
            topologyManager.UpdateOperatorSettings(topologyUnit.PrimaryKey, operatorSettings);
            return Task.CompletedTask;
        }

        public Task RemoveCustomeOperators(Guid guid)
        {
            int index = -1;
            for (int i = 0; i < statefulOperators.Count; i++)
            {
                if (statefulOperators[i].GetPrimaryKey() == guid)
                {
                    index = i;
                    break;
                }
            }
            if (index != -1)
            {
                statefulOperators.RemoveAt(index);
                PrettyConsole.Line("Remove old stateful from upper stream");
                operatorSettings.RemoveOperatorFromDict(guid);
                topologyManager.UpdateOperatorSettings(this.GetPrimaryKey(), operatorSettings);
            }
            else
            {
                throw new ArgumentException();
            }

            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public Task ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            //At first split text into words
            if (msg.Key != Constants.System_Key)
            {
                IncrementUpStreamCount(msg);
                msg.From = this.GetPrimaryKey();
                CustomExcutionMethod(msg, stream);
            }
            else
            {
                ProcessSpecialMessageAsync(msg, stream);
            }
            return Task.CompletedTask;
        }

        private Task IncrementUpStreamCount(StreamMessage msg)
        {
            if (upStreamMessageCountMap.ContainsKey(msg.From))
            {
                upStreamMessageCountMap[msg.From] = upStreamMessageCountMap[msg.From] + 1;
            }
            else
            {
                upStreamMessageCountMap.Add(msg.From, 1);
            }
            return Task.CompletedTask;
        }

        public abstract Task<Task> CustomExcutionMethod(StreamMessage msg, IAsyncStream<StreamMessage> stream);

        protected async Task<Task> ProcessSpecialMessageAsync(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            BarrierOrCommitMsgTrackingInfo info = new BarrierOrCommitMsgTrackingInfo(msg.barrierOrCommitInfo.GetID(), msg.barrierOrCommitInfo.numberOfClientSent);
            info.BatchID = msg.BatchID;
            if (msg.Value == Constants.Barrier_Value)
            {
                if (CheckCount(msg))
                {
                    ResetCountMap(upStreamMessageCountMap);
                    await HandleBarrierMessages(msg);
                    await batchTracker.CompleteOneOperatorBarrier(info);
                }
            }
            else
            {
                throw new NotImplementedException("Unrecgonizable system message in stateless operator");
            }
            await BroadcastSpecialMessage(msg, stream);
            return Task.CompletedTask;
        }

        private bool CheckCount(StreamMessage msg)
        {
            if (upStreamMessageCountMap.ContainsKey(msg.From) && msg.Count == upStreamMessageCountMap[msg.From])
            {
                return true;
            }
            else if (!upStreamMessageCountMap.ContainsKey(msg.From) && msg.Count == 0)
            {
                return true;
            }
            else
            {
                PrettyConsole.Line("The count in stateless operator is not equal!");
                return false;
            }
        }

        private void ResetCountMap(Dictionary<Guid, int> countMap)
        {
            var keys = countMap.Keys.ToList();
            for (int i = 0; i < keys.Count; i++)
            {
                countMap[keys[i]] = 0;
            }
        }

        private Task HandleBarrierMessages(StreamMessage msg)
        {
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), statefulOperators.Count);
            msg.barrierOrCommitInfo.BatchID = msg.BatchID;
            if (batchTracker != null)
            {
                //PrettyConsole.Line("Tracking with stateless with " + statefulOperators.Count);
                batchTracker.TrackingBarrierMessages(msg);
            }
            else
            {
                throw new NullReferenceException();
            }
            return Task.CompletedTask;
        }

        //Commit Logic
        public Task Commit(StreamMessage msg)
        {
            //tell the tracker commit is done in this operator
            PrettyConsole.Line("Start commit in stateless");
            batchTracker.CompleteOneOperatorCommit(msg.barrierOrCommitInfo);
            PrettyConsole.Line("Here");
            return Task.CompletedTask;
        }

        public Task Recovery(StreamMessage msg)
        {
            batchTracker.CompleteOneOperatorRecovery(msg.barrierOrCommitInfo);
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

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        protected async Task<Task> ExecuteMessagesByDownStreamOperators(StreamMessage msg, IAsyncStream<StreamMessage> stream, IStatefulOperator statefulOperator, int index)
        {
            try
            {
                msg.From = this.GetPrimaryKey();
                if (msg.Value == Constants.Barrier_Value)
                {
                    if (downStreamMessageCountMap.ContainsKey(statefulOperator.GetPrimaryKey()))
                    {
                        msg.Count = downStreamMessageCountMap[statefulOperator.GetPrimaryKey()];
                    }
                    else
                    {
                        msg.Count = 0;
                    }
                }
                else if(msg.Value != Constants.System_Key)
                {
                    var key = statefulOperator.GetPrimaryKey();
                    if (downStreamMessageCountMap.ContainsKey(key))
                    {
                        downStreamMessageCountMap[key] = downStreamMessageCountMap[key] + 1;
                    }
                    else
                    {
                        downStreamMessageCountMap.Add(key, 1);
                    }
                }
                await statefulOperator.ExecuteMessage(msg, stream);
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                PrettyConsole.Line("Get Exception : " + e.GetType() + "; Start Receovry");
                //1. Restart a new grain
                IStatefulOperator newOperator = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
                //2. Rollback the state
                //a. Find the operator settings
                //The method has to be called becasue grain initialize after the method call.
                await newOperator.MarkOperatorAsFailed();
                var item = operatorSettings.GetOperatorDict().ElementAt(index);
                //b. Load the setting 
                //await newOperator.LoadSettings(item.Value);
                //c. mark as failed, so when it receive recovery message it will 
                //revert states by incremental log
                //await newOperator.MarkOperatorAsFailed();
                //3. Remove the failed from the topology
                //statefulOperators.RemoveAt(index);
                //operatorSettings.RemoveOperatorFromDict(item.Key);
                //4. Add the new grain to topology
                //statefulOperators.Add(newOperator);
                // operatorSettings.AddOpratorToDict(newOperator.GetPrimaryKey(), await newOperator.GetOperatorSettings());
                //await topologyManager.UpdateOperatorSettings(this.GetPrimaryKey(), operatorSettings);
                //Since the new operator will add itself to the topology it self, so it is ok
                await topologyManager.ReplaceTheOldOperatorWithNew(item.Key, newOperator.GetPrimaryKey());
                //5. Remove the old from the topology
                await topologyManager.RemoveUnit(item.Key);
                //6. Replace the new operator

                //8. Start Recovery
                var batchCoordinator = GrainFactory.GetGrain<IBatchCoordinator>(Constants.Coordinator);
                await batchCoordinator.StartRecovery();
                return Task.CompletedTask;
            }
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

        public Task<TopologyUnit> GetTopologyUnit()
        {
            return Task.FromResult(topologyUnit);
        }
    }
}
