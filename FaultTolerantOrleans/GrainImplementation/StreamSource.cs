using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

namespace GrainImplementation
{
	public class StreamSource : Grain, IStreamSource
	{
        //Information that used for Application testsing 
        private readonly List<StreamMessage> messages = new List<StreamMessage>(100);
		private readonly List<string> onlineMembers = new List<string>(10);

        //Internal Implementation
        private List<IOperator> downStreamOperators = new List<IOperator>();
        private IBatchCoordinator batchCoordinator;
        private IBatchTracker batchTracker;
		private IAsyncStream<StreamMessage> stream;
        private ITopology topologyManager;

        private List<StreamMessage> messageBuffer = new List<StreamMessage>();
        private Dictionary<int, Dictionary<Guid, int>> messageCountMaps = new Dictionary<int, Dictionary<Guid, int>>(); 
        private TopologyUnit topologyUnit;
        protected OperatorSettings operatorSettings = new OperatorSettings();
        private int currentBatchID;
        private Guid testAddNewOperatorGuid;
        private int roundRobinValue = 0;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public override Task OnActivateAsync()
		{
			var streamProvider = GetStreamProvider(Constants.FaultTolerantStreamProvider);
            stream = streamProvider.GetStream<StreamMessage>(Guid.NewGuid(), Constants.FaultTolerantStreamNameSpace);
            SetUpBatchManager();
            SetUpBatchTracker();
            currentBatchID = 0;
            topologyManager = GrainFactory.GetGrain<ITopology>(Constants.Topology_Manager);
            topologyUnit = new TopologyUnit(OperatorType.Source, this.GetPrimaryKey());
            topologyManager.AddUnit(topologyUnit);
          return base.OnActivateAsync();
		}

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public Task InitDeaultOperators()
        {
            var operatorOne = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid(), Constants.Stateless_Operator_Prefix);
            var operatorTwo = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid(), Constants.Stateless_Operator_Prefix);
            var operatorThree = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid(), Constants.Stateless_Operator_Prefix);
            //If too many use for loop
            downStreamOperators.Add(operatorOne);
            downStreamOperators.Add(operatorTwo);
            downStreamOperators.Add(operatorThree);
            
            //Add custom guid list to its child
            List<TopologyUnit> units = new List<TopologyUnit>();
            units.Add(new TopologyUnit(OperatorType.Stateful, Guid.NewGuid()));
            units.Add(new TopologyUnit(OperatorType.Stateful, Guid.NewGuid()));

            operatorOne.InitRandomOperators();
            operatorTwo.AddCustomDownStreamOperators(units);
            operatorThree.AddCustomDownStreamOperators(units);
            testAddNewOperatorGuid = operatorTwo.GetPrimaryKey();
            //Add the units to the topology
            InitTopology();

            return Task.CompletedTask;
        }

        private Task InitTopology()
        {
            foreach (var item in downStreamOperators)
            {
                topologyManager.ConnectUnits(topologyUnit.PrimaryKey, item.GetPrimaryKey());
            }
            return Task.CompletedTask;
        }

        public Task<Guid> GetTestGuid()
        {
            return Task.FromResult(testAddNewOperatorGuid);
        }

        public Task AddCustomDownStreamOperator(IStatelessOperator statelessOperator)
        {
            //Add to local
            downStreamOperators.Add(statelessOperator);
            //Add it to global
            topologyManager.ConnectUnits(topologyUnit.PrimaryKey, statelessOperator.GetPrimaryKey());
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
            testAddNewOperatorGuid = units[0].PrimaryKey;
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
            }
            else
            {
                throw new ArgumentException();
            }

            return Task.CompletedTask;
        }

        private Task SetUpBatchManager()
        {
            batchCoordinator = GrainFactory.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            batchCoordinator.AddSourceAndRegisterTimer(stream, this);

            return Task.CompletedTask;
        }

        private Task SetUpBatchTracker()
        {
            batchTracker = GrainFactory.GetGrain<IBatchTracker>(Constants.Tracker);
            return Task.CompletedTask;
        }

		public Task<Guid> Join(string nickname)
		{
			onlineMembers.Add(nickname);
			return Task.FromResult(stream.Guid);
		}

		public async Task<bool> Message(StreamMessage msg)
		{
			messages.Add(msg);
            await ProduceMessageAsync(msg);
			return true;
		}

         public async Task<Task> ProduceMessageAsync(StreamMessage msg)
        {
            msg.From = topologyUnit.PrimaryKey;
            if (msg.Key != Constants.System_Key)
            {
                msg.Start_Time = DateTime.Now.Millisecond;
                if (msg.messageType != MessageType.Test)
                {
                    msg.BatchID = currentBatchID;
                }
                messageBuffer.Add(msg);
                ProcessNormalMessage(msg);
            }
            else
            {
                ProcessSpecialMessage(msg, stream);
            }
            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        private Task ProcessNormalMessage(StreamMessage msg)
        {
            //At first find a operator by hashing
            int index = roundRobinValue % downStreamOperators.Count;
            roundRobinValue++;
            CheckIfOutOfBoundry(downStreamOperators, index);
            var op = downStreamOperators[index];
            IncrementCountMap(op.GetPrimaryKey(), msg.BatchID);
            op.ExecuteMessage(msg, stream);

            return Task.CompletedTask;
        }

        private void CheckIfOutOfBoundry(List<IOperator> list, int index)
        {
            if (index >= list.Count)
            {
                throw new ArgumentException("The list in streamSouce is out of boundrary");
            }
        }

        private void IncrementCountMap(Guid key, int batchID)
        {
            if (!messageCountMaps.ContainsKey(batchID))
            {
                messageCountMaps.Add(batchID, new Dictionary<Guid, int>());
            }

            if (messageCountMaps[batchID].ContainsKey(key))
            {
                messageCountMaps[batchID][key] = messageCountMaps[batchID][key] + 1;
            }
            else
            {
                messageCountMaps[batchID].Add(key, 1);
            }
        }

        //If it is special message, it has to send to all the operators. 
        //If it is barrier message, batch manager will start to track it
        //by using BarrierMsgTrackingInfo which keep and ID and the number of 
        //client it sent to. 
        private async Task<Task> ProcessSpecialMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            BarrierOrCommitMsgTrackingInfo info = new BarrierOrCommitMsgTrackingInfo(msg.barrierOrCommitInfo.GetID(), msg.barrierOrCommitInfo.numberOfClientSent);
            info.BatchID = msg.BatchID;
            if (msg.Value == Constants.Barrier_Value && msg.BatchID - currentBatchID < 2)
            {
                currentBatchID = msg.BatchID + 1;
                PrettyConsole.Line("Increment ID " + currentBatchID);
                await TrackingBarrierMessages(msg);
                batchTracker.CompleteOneOperatorBarrier(info);
                BroadcastSpecialMessage(msg, stream);
            }
            return Task.CompletedTask;
        }

        //Commit Logic
        public Task Commit(StreamMessage msg)
        {
            //tell the tracker commit is done in this operator
            batchTracker.CompleteOneOperatorCommit(msg.barrierOrCommitInfo);
            //Clean the buffer
            messageBuffer.Clear();
            if (messageCountMaps.ContainsKey(msg.BatchID))
            {
                messageCountMaps.Remove(msg.BatchID);
            }
            return Task.CompletedTask;
        }

        //Recovery Logic
        public Task Recovery(StreamMessage msg)
        {
            currentBatchID = msg.BatchID + 1;
            //tell the tracker recovery is done in this operator
            batchTracker.CompleteOneOperatorRecovery(msg.barrierOrCommitInfo);
            return Task.CompletedTask;
        }

        private Task TrackingBarrierMessages(StreamMessage msg)
        {
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), downStreamOperators.Count);
            batchTracker.TrackingBarrierMessages(msg);
            return Task.CompletedTask;
        }

        private Task BroadcastSpecialMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            foreach(IOperator item in downStreamOperators)
            {
                if (messageCountMaps.ContainsKey(msg.BatchID))
                {
                    if (messageCountMaps[msg.BatchID].ContainsKey(item.GetPrimaryKey()))
                    {
                        msg.Count = messageCountMaps[msg.BatchID][item.GetPrimaryKey()];
                    }
                    else
                    {
                        msg.Count = 0;
                    }
                }
                //If not contain, means it does not receive any messages of
                //that batch
                else
                {
                    msg.Count = 0;    
                }
                item.ExecuteMessage(msg, stream);
            }
            return Task.CompletedTask;
        }

        //Replay Logic
        public async Task<Task> ReplayTheMessageOnRecoveryCompleted()
        {
            PrettyConsole.Line("Start Replay!");
            foreach (StreamMessage msg in messageBuffer)
            {
                ProcessNormalMessage(msg);
            }
            return Task.CompletedTask;
        }

        public Task<string[]> GetMembers()
	    {
	        return Task.FromResult(onlineMembers.ToArray());
	    }

        public Task<IBatchCoordinator> GetBatchManager()
        {
            return Task.FromResult(batchCoordinator);
        }

        public Task<IBatchTracker> GetBatchTracker()
        {
            return Task.FromResult(batchTracker);
        }

        public async Task<int> GetState(StreamMessage msg)
        {
            foreach(var op in downStreamOperators)
            {
                var count = await op.GetState(msg.Value);
                if (count != -1)
                {
                    return await Task.FromResult(count);
                }
            }
            return await Task.FromResult(-2);
        }

        public async Task<int> GetStateInReverseLog(StreamMessage msg)
        {
            foreach (var op in downStreamOperators)
            {
                var count = await op.GetStateInReverseLog(msg.Value);
                if (count != -1)
                {
                    return await Task.FromResult(count);
                }
            }
            return await Task.FromResult(-2);
        }

        public async Task<int> GetStateInIncrementalLog(StreamMessage msg)
        {
            foreach (var op in downStreamOperators)
            {
                var count = await op.GetStateInIncrementalLog(msg.Value);
                if (count != -1)
                {
                    return await Task.FromResult(count);
                }
            }
            return await Task.FromResult(-2);
        }

        public Task<TopologyUnit> GetTopologyUnit()
        {
            return Task.FromResult(topologyUnit);
        }

        public Task<int> DetectErrors()
        {
            return Task.FromResult(1);
        }

    }
}