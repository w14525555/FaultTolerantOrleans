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
        private readonly List<StreamMessage> messages = new List<StreamMessage>(100);
		private readonly List<string> onlineMembers = new List<string>(10);
        private HashSet<IStatelessOperator> statelessOperators;

        private IBatchCoordinator batchCoordinator;
        private IBatchTracker batchTracker;
		private IAsyncStream<StreamMessage> stream;
        private int currentBatchID;

        public override Task OnActivateAsync()
		{
			var streamProvider = GetStreamProvider(Constants.ChatRoomStreamProvider);
            stream = streamProvider.GetStream<StreamMessage>(Guid.NewGuid(), Constants.CharRoomStreamNameSpace);
            SetUpBatchManager();
            SetUpBatchTracker();
            currentBatchID = 0;
            InitOperators();
          return base.OnActivateAsync();
		}

        private Task InitOperators()
        {
            statelessOperators = new HashSet<IStatelessOperator>();
            var operatorOne = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid());
            var operatorTwo = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid());
            var operatorThree = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid());
            //If too many use for loop
            operatorOne.SetBatchTracker(batchTracker);
            operatorTwo.SetBatchTracker(batchTracker);
            operatorThree.SetBatchTracker(batchTracker);
            statelessOperators.Add(operatorOne);
            statelessOperators.Add(operatorTwo);
            statelessOperators.Add(operatorThree);
            return Task.CompletedTask;
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
            batchTracker.SetBatchManager(batchCoordinator);
            return Task.CompletedTask;
        }

		public async Task<Guid> Join(string nickname)
		{
			onlineMembers.Add(nickname);
            await ProduceMessageAsync(new StreamMessage("System", $"{nickname} joins the chat '{this.GetPrimaryKeyString()}' ..."));

			return stream.Guid;
		}

		public async Task<Guid> Leave(string nickname)
		{
			onlineMembers.Remove(nickname);

            await ProduceMessageAsync(new StreamMessage("System", $"{nickname} leaves the chat..."));

			return stream.Guid;
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
                //At first find a operator by hashing
                if (msg.messageType != MessageType.Test)
                {
                    msg.BatchID = currentBatchID;
                }
                IStatelessOperator statelessOp = await SystemImplementation.PartitionFunction.PartitionStatelessByKey(msg.Key, statelessOperators);
                await statelessOp.ExecuteMessage(msg, stream);
            }
            else
            {
                await ProcessSpecialMessage(msg, stream);
            }
            return Task.CompletedTask;
        }


        //If it is special message, it has to send to all the operators. 
        //If it is barrier message, batch manager will start to track it
        //by using BarrierMsgTrackingInfo which keep and ID and the number of 
        //client it sent to. 
        private async Task<Task> ProcessSpecialMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            if (msg.Value == Constants.Barrier_Value)
            {
                BarrierMsgTrackingInfo info = new BarrierMsgTrackingInfo(msg.barrierInfo.GetID(), msg.barrierInfo.numberOfClientSent);
                info.BatchID = msg.BatchID;
                await HandleBarrierMessages(msg);
                await batchTracker.CompleteTracking(info);
                await BroadcastSpecialMessage(msg, stream);
            }
            //The source just broadcast the commit message
            else if (msg.Value == Constants.Commit_Value)
            {
                PrettyConsole.Line("Send comit message for BatchID: " + msg.BatchID);
                await BroadcastSpecialMessage(msg, stream);
            }
            return Task.CompletedTask;
        }

        private Task HandleBarrierMessages(StreamMessage msg)
        {
            currentBatchID = msg.BatchID + 1;
            msg.barrierInfo = new BarrierMsgTrackingInfo(Guid.NewGuid(), statelessOperators.Count);
            msg.barrierInfo.BatchID = msg.BatchID;
            PrettyConsole.Line("Tracking Batch " + msg.BatchID + " with " + statelessOperators.Count);
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
    }
}