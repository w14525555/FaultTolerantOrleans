using System;
using System.Collections.Generic;
using System.Linq;
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
        private Dictionary<Guid, int> messageCountMap = new Dictionary<Guid, int>(); 
        private TopologyUnit topologyUnit;
        protected OperatorSettings operatorSettings = new OperatorSettings();
        private int currentBatchID;
        private Guid testAddNewOperatorGuid;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public override Task OnActivateAsync()
		{
			var streamProvider = GetStreamProvider(Constants.FaultTolerantStreamProvider);
            stream = streamProvider.GetStream<StreamMessage>(Guid.NewGuid(), Constants.FaultTolerantStreamNameSpace);
            SetUpBatchManager();
            SetUpBatchTracker();
            currentBatchID = 0;
            topologyManager = GrainFactory.GetGrain<ITopology>(Constants.Topology_Manager);
            topologyUnit = new TopologyUnit(OperatorType.Source, Guid.NewGuid());
            topologyUnit.SetSourceKey(this.GetPrimaryKeyString());
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
            //Add the down stream operators to the count map
            InitCountMap();

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

        private Task InitCountMap()
        {
            foreach(var item in downStreamOperators)
            {
                messageCountMap.Add(item.GetPrimaryKey(), 0);
            }
            return Task.CompletedTask;
        }

        public Task AddCustomDownStreamOperator(IStatelessOperator statelessOperator)
        {
            //Add to local
            downStreamOperators.Add(statelessOperator);
            //Add it to global
            topologyManager.ConnectUnits(topologyUnit.PrimaryKey, statelessOperator.GetPrimaryKey());
            //Add it to counter map
            messageCountMap.Add(statelessOperator.GetPrimaryKey(), 0);
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
            }
            else
            {
                throw new ArgumentException();
            }

            return Task.CompletedTask;
        }

        public Task<int> GetNumberOfElementsInCountMap()
        {
            return Task.FromResult(messageCountMap.Count);
        }

        private Task SetUpBatchManager()
        {
            batchCoordinator = GrainFactory.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            batchCoordinator.SetChannelAndRegisterTimer(stream, this);

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

        //This method has to be async because you have to wait
        //the stream sends messages to all its subscribers
         public async Task<Task> ProduceMessageAsync(StreamMessage msg)
        {
            msg.From = topologyUnit.PrimaryKey;
            if (msg.Key != Constants.System_Key)
            {
                if (msg.messageType != MessageType.Test)
                {
                    msg.BatchID = currentBatchID;
                }
                messageBuffer.Add(msg);
                await ProcessNormalMessage(msg);
            }
            else
            {
                await ProcessSpecialMessage(msg, stream);
            }
            return Task.CompletedTask;
        }

        private async Task<Task> ProcessNormalMessage(StreamMessage msg)
        {
            //At first find a operator by hashing
            IOperator op = await SystemImplementation.PartitionFunction.PartitionStatelessByKey(msg.Key, downStreamOperators);
            await IncrementCountMap(op.GetPrimaryKey());
            await op.ExecuteMessage(msg, stream);

            return Task.CompletedTask;
        }

        private Task IncrementCountMap(Guid key)
        {
            if (messageCountMap.ContainsKey(key))
            {
                messageCountMap[key] = messageCountMap[key] + 1;
                return Task.CompletedTask;
            }
            else
            {
                throw new InvalidOperationException("Something wrong! The count map does not contains a key of its down stream");
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
            if (msg.Value == Constants.Barrier_Value)
            {
                currentBatchID = msg.BatchID + 1;
                await TrackingBarrierMessages(msg);
                await batchTracker.CompleteOneOperatorBarrier(info);
                await BroadcastSpecialMessage(msg, stream);
            }
            return Task.CompletedTask;
        }

        //Commit Logic
        public Task Commit(StreamMessage msg)
        {
            //Clean the buffer
            messageBuffer.Clear();
            //tell the tracker commit is done in this operator
            batchTracker.CompleteOneOperatorCommit(msg.barrierOrCommitInfo);
            return Task.CompletedTask;
        }

        //Commit Logic
        public Task Recovery(StreamMessage msg)
        {
            currentBatchID = msg.BatchID;
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
            foreach(IStatelessOperator item in downStreamOperators)
            {
                if (messageCountMap.ContainsKey(item.GetPrimaryKey()))
                {
                    msg.Count = messageCountMap[item.GetPrimaryKey()];
                }
                else
                {
                    throw new InvalidOperationException("Message count map does not contain the key");
                }
                item.ExecuteMessage(msg, stream);
            }
            ResetCountMap();
            return Task.CompletedTask;
        }

        private void ResetCountMap()
        {
            var keys = messageCountMap.Keys.ToList();
            for(int i = 0; i < keys.Count; i++)
            {
                messageCountMap[keys[i]] = 0;
            }
        }

        //Replay Logic
        public async Task<Task> ReplayTheMessageOnRecoveryCompleted()
        {
            PrettyConsole.Line("Start Replay!");
            foreach (StreamMessage msg in messageBuffer)
            {
                await ProcessNormalMessage(msg);
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

    }
}