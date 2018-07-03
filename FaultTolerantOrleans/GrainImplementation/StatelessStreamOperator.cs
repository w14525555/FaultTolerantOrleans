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
        private Dictionary<int, Dictionary<Guid, int>> upStreamMessageCountMaps = new Dictionary<int, Dictionary<Guid, int>>();
        private Dictionary<int, Dictionary<Guid, int>> downStreamMessageCountMaps = new Dictionary<int, Dictionary<Guid, int>>();

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

        public Task RemoveCustomDownStreamOperator(Guid guid)
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
                await CustomExcutionMethod(msg, stream);
            }
            else
            {
                ProcessSpecialMessageAsync(msg, stream);
            }
            return Task.CompletedTask;
        }

        private Task IncrementUpStreamCount(StreamMessage msg)
        {
            int batchID = msg.BatchID;
            if (!upStreamMessageCountMaps.ContainsKey(batchID))
            {
                upStreamMessageCountMaps.Add(batchID, new Dictionary<Guid, int>());
            }

            if (upStreamMessageCountMaps[batchID].ContainsKey(msg.From))
            {
                upStreamMessageCountMaps[batchID][msg.From] = upStreamMessageCountMaps[batchID][msg.From] + 1;
            }
            else
            {
                upStreamMessageCountMaps[batchID].Add(msg.From, 1);
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
            if (!upStreamMessageCountMaps.ContainsKey(msg.BatchID))
            {
                if (msg.Count == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else if (upStreamMessageCountMaps[msg.BatchID].ContainsKey(msg.From) && msg.Count == upStreamMessageCountMaps[msg.BatchID][msg.From])
            {
                return true;
            }
            else if (!upStreamMessageCountMaps[msg.BatchID].ContainsKey(msg.From) && msg.Count == 0)
            {
                return true;
            }
            else
            {
                PrettyConsole.Line("The count in stateless operator is not equal!");
                return false;
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
            if (upStreamMessageCountMaps.ContainsKey(msg.BatchID))
            {
                upStreamMessageCountMaps.Remove(msg.BatchID);
            }
            if (downStreamMessageCountMaps.ContainsKey(msg.BatchID))
            {
                downStreamMessageCountMaps.Remove(msg.BatchID);
            }
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
            if (downStreamOperators.Count > 0)
            {
                foreach (IStatefulOperator item in downStreamOperators)
                {
                    await ExecuteMessagesByDownStreamOperators(msg, stream, item);
                }
            }
            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        protected async Task<Task> ExecuteMessagesByDownStreamOperators(StreamMessage msg, IAsyncStream<StreamMessage> stream, IOperator op)
        {
            if (downStreamOperators.Count > 0)
            {
                int batchID = msg.BatchID;
                var targetKey = op.GetPrimaryKey();
                try
                {
                    msg.From = this.GetPrimaryKey();
                    //if is barrier message, set the message count
                    if (msg.Value == Constants.Barrier_Value)
                    {
                        if (downStreamMessageCountMaps.ContainsKey(batchID))
                        {
                            if (downStreamMessageCountMaps[batchID].ContainsKey(op.GetPrimaryKey()))
                            {
                                msg.Count = downStreamMessageCountMaps[batchID][op.GetPrimaryKey()];
                            }
                            else
                            {
                                msg.Count = 0;
                            }
                        }
                        else
                        {
                            msg.Count = 0;
                        }
                    }
                    //if it is a normal message, increment the count map
                    else if (msg.Value != Constants.System_Key)
                    {
                        if (!downStreamMessageCountMaps.ContainsKey(batchID))
                        {
                            downStreamMessageCountMaps.Add(batchID, new Dictionary<Guid, int>());
                        }

                        var key = op.GetPrimaryKey();
                        if (downStreamMessageCountMaps[batchID].ContainsKey(key))
                        {
                            downStreamMessageCountMaps[batchID][key] = downStreamMessageCountMaps[batchID][key] + 1;
                        }
                        else
                        {
                            downStreamMessageCountMaps[batchID].Add(key, 1);
                        }
                    }
                    await op.ExecuteMessage(msg, stream);
                }
                catch (Exception e)
                {
                    PrettyConsole.Line("Get Exception : " + e + "; Start Receovry");
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
                }
            }
            return Task.CompletedTask;
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
