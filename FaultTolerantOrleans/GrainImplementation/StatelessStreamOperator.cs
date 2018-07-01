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
            
        protected List<IOperator> downStreamOperators = new List<IOperator>();
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
            IStatefulOperator operatorOne = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
            IStatefulOperator operatorTwo = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
            downStreamOperators.Add(operatorOne);
            operatorOne.IncrementNumberOfUpStreamOperator();
            downStreamOperators.Add(operatorTwo);
            operatorTwo.IncrementNumberOfUpStreamOperator();
            operatorSettings.AddOpratorToDict(operatorOne.GetPrimaryKey(), await operatorOne.GetOperatorSettings());
            operatorSettings.AddOpratorToDict(operatorTwo.GetPrimaryKey(), await operatorTwo.GetOperatorSettings());

            topologyManager.UpdateOperatorSettings(this.GetPrimaryKey(), operatorSettings);
            topologyManager.ConnectUnits(this.GetPrimaryKey(), operatorOne.GetPrimaryKey());
            topologyManager.ConnectUnits(this.GetPrimaryKey(), operatorTwo.GetPrimaryKey());

            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> AddCustomDownStreamOperators(List<TopologyUnit> units)
        {
            foreach (var unit in units)
            {
                if (unit.OperatorType == OperatorType.Stateful)
                {
                    var op = GrainFactory.GetGrain<IStatefulOperator>(unit.PrimaryKey, Constants.Stateful_Operator_Prefix);
                    op.IncrementNumberOfUpStreamOperator();
                    downStreamOperators.Add(op);
                    operatorSettings.AddOpratorToDict(op.GetPrimaryKey(), await op.GetOperatorSettings());
                    topologyManager.ConnectUnits(topologyUnit.PrimaryKey, op.GetPrimaryKey());
                }
                else if (unit.OperatorType == OperatorType.Stateless)
                {
                    var op = GrainFactory.GetGrain<IStatelessOperator>(unit.PrimaryKey, Constants.Stateless_Operator_Prefix);
                    downStreamOperators.Add(op);
                    topologyManager.ConnectUnits(topologyUnit.PrimaryKey, op.GetPrimaryKey());
                }
                else
                {
                    throw new ArgumentException("The down stream operor cannot be a source");
                }
            }
            topologyManager.UpdateOperatorSettings(topologyUnit.PrimaryKey, operatorSettings);
            return Task.CompletedTask;
        }

        public Task RemoveCustomDownStreamOperators(Guid guid)
        {
            int index = -1;
            for (int i = 0; i < downStreamOperators.Count; i++)
            {
                if (downStreamOperators[i].GetPrimaryKey() == guid)
                {
                    index = i;
                    break;
                }
            }
            if (index != -1)
            {
                downStreamOperators.RemoveAt(index);
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
        public async Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            //At first split text into words
            if (msg.Key != Constants.System_Key)
            {
                await IncrementUpStreamCount(msg);
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
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), downStreamOperators.Count);
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
            batchTracker.CompleteOneOperatorCommit(msg.barrierOrCommitInfo);
            return Task.CompletedTask;
        }

        public Task Recovery(StreamMessage msg)
        {
            batchTracker.CompleteOneOperatorRecovery(msg.barrierOrCommitInfo);
            return Task.CompletedTask;
        }

        private async Task<Task> BroadcastSpecialMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            foreach (IStatefulOperator item in downStreamOperators)
            {
                await ExecuteMessagesByDownStreamOperators(msg, stream, item);
            }
            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        protected async Task<Task> ExecuteMessagesByDownStreamOperators(StreamMessage msg, IAsyncStream<StreamMessage> stream, IOperator op)
        {
            var targetKey = op.GetPrimaryKey();
            try
            {
                msg.From = this.GetPrimaryKey();
                if (msg.Value == Constants.Barrier_Value)
                {
                    if (downStreamMessageCountMap.ContainsKey(op.GetPrimaryKey()))
                    {
                        msg.Count = downStreamMessageCountMap[op.GetPrimaryKey()];
                    }
                    else
                    {
                        msg.Count = 0;
                    }
                }
                else if(msg.Value != Constants.System_Key)
                {
                    var key = op.GetPrimaryKey();
                    if (downStreamMessageCountMap.ContainsKey(key))
                    {
                        downStreamMessageCountMap[key] = downStreamMessageCountMap[key] + 1;
                    }
                    else
                    {
                        downStreamMessageCountMap.Add(key, 1);
                    }
                }
                await op.ExecuteMessage(msg, stream);
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                PrettyConsole.Line("Get Exception : " + e.GetType() + "; Start Receovry");
                //1. Restart a new grain
                IStatefulOperator newOperator = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid());
                //2. make the new one as failed grain
                await newOperator.MarkOperatorAsFailed();
                //3. Replace the old by new 
                await topologyManager.ReplaceTheOldOperatorWithNew(targetKey, newOperator.GetPrimaryKey());
                //4. Remove the old from the topology
                await topologyManager.RemoveUnit(targetKey);
                //5. Start Recovery
                var batchCoordinator = GrainFactory.GetGrain<IBatchCoordinator>(Constants.Coordinator);
                await batchCoordinator.StartRecovery();
                return Task.CompletedTask;
            }
        }

        public async Task<int> GetState(string word)
        {
            foreach(var op in downStreamOperators)
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
            foreach (var op in downStreamOperators)
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
            foreach (var op in downStreamOperators)
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
