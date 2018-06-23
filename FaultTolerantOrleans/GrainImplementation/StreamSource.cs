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
        private HashSet<IStatelessOperator> statelessOperators;
        private HashSet<IStatefulOperator> statefulOperators;

        private IBatchCoordinator batchCoordinator;
        private IBatchTracker batchTracker;
		private IAsyncStream<StreamMessage> stream;
        private ITopology topologyManager;

        private List<StreamMessage> messageBuffer = new List<StreamMessage>();
        //A map that uses to ensure exactly once 
        private Dictionary<Guid, int> messageCountMap = new Dictionary<Guid, int>(); 
        private TopologyUnit topologyUnit;
        private int currentBatchID;

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
            InitOperators();
            statefulOperators = new HashSet<IStatefulOperator>();
          return base.OnActivateAsync();
		}

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        private Task InitOperators()
        {
            statelessOperators = new HashSet<IStatelessOperator>();
            var operatorOne = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid(), Constants.Stateless_Operator_Prefix);
            var operatorTwo = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid(), Constants.Stateless_Operator_Prefix);
            var operatorThree = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid(), Constants.Stateless_Operator_Prefix);
            //If too many use for loop
            statelessOperators.Add(operatorOne);
            statelessOperators.Add(operatorTwo);
            statelessOperators.Add(operatorThree);
            
            //Add custom guid list
            List<Guid> guidList = new List<Guid>();
            guidList.Add(Guid.NewGuid());
            guidList.Add(Guid.NewGuid());

            operatorOne.InitRandomOperators();
            operatorTwo.AddCustomeOperators(guidList);
            operatorThree.AddCustomeOperators(guidList);

            //Add the units to the topology
            InitTopology();

            //Add the down stream operators to the count map
            InitCountMap();

            return Task.CompletedTask;
        }

        private Task InitTopology()
        {
            foreach (var item in statelessOperators)
            {
                topologyManager.ConnectUnits(topologyUnit.PrimaryKey, item.GetPrimaryKey());
            }
            return Task.CompletedTask;
        }


        private Task InitCountMap()
        {
            foreach(var item in statelessOperators)
            {
                messageCountMap.Add(item.GetPrimaryKey(), 0);
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

		public Task<Guid> Leave(string nickname)
		{
			onlineMembers.Remove(nickname);

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
            IStatelessOperator statelessOp = await SystemImplementation.PartitionFunction.PartitionStatelessByKey(msg.Key, statelessOperators);
            msg.From = topologyUnit.PrimaryKey;
            await IncrementCountMap(statelessOp.GetPrimaryKey());
            await statelessOp.ExecuteMessage(msg, stream);

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
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), statelessOperators.Count);
            batchTracker.TrackingBarrierMessages(msg);
            return Task.CompletedTask;
        }

        private Task BroadcastSpecialMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            foreach(IStatelessOperator item in statelessOperators)
            {
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
                await ProcessNormalMessage(msg);
            }
            return Task.CompletedTask;
        }

        public Task<string[]> GetMembers()
	    {
	        return Task.FromResult(onlineMembers.ToArray());
	    }

	    public Task<StreamMessage[]> ReadHistory(int numberOfMessages)
	    {
	        var response = messages
	            .OrderByDescending(x => x.Created)
	            .Take(numberOfMessages)
	            .OrderBy(x => x.Created)
	            .ToArray();

	        return Task.FromResult(response);
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
            foreach(var op in statelessOperators)
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
            foreach (var op in statelessOperators)
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
            foreach (var op in statelessOperators)
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