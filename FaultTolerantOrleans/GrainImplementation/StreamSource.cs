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
        private StreamMessage barrierMsg = new StreamMessage(Constants.Barrier_Key, Constants.System_Value);
        private StreamMessage commitMsg = new StreamMessage(Constants.Commit_Key, Constants.System_Value);
        private readonly List<StreamMessage> messages = new List<StreamMessage>(100);
		private readonly List<string> onlineMembers = new List<string>(10);
        private HashSet<IStatelessOperator> statelessOperators;

        private IBatchCoordinator batchManager;
        private IBatchTracker batchTracker;
		private IAsyncStream<StreamMessage> stream;
        private int currentBatchID;
        private string downStreamStatelessOne = "statelessOne";
        private string downStreamStatelessTwo = "statelessTwo";
        private string downStreamStatelessThree = "statelessThree";

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
            statelessOperators.Add(GrainFactory.GetGrain<IStatelessOperator>(downStreamStatelessOne));
            statelessOperators.Add(GrainFactory.GetGrain<IStatelessOperator>(downStreamStatelessTwo));
            statelessOperators.Add(GrainFactory.GetGrain<IStatelessOperator>(downStreamStatelessThree));
            return Task.CompletedTask;
        }

        private Task SetUpBatchManager()
        {
            batchManager = GrainFactory.GetGrain<IBatchCoordinator>("Manager");
            batchManager.SetChannelAndRegisterTimer(stream, this);

            return Task.CompletedTask;
        }

        private Task SetUpBatchTracker()
        {
            batchTracker = GrainFactory.GetGrain<IBatchTracker>("Tracker");
            batchTracker.SetBatchManager(batchManager);
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
            if (msg.Key != "System")
            {
                await CheckIfBarrierOrCommitMsg(msg);
                //At first find a operator by hashing
                IStatelessOperator statelessOp = await SystemImplementation.PartitionFunction.PartitionStatelessByKey(msg.Key, statelessOperators);
                await statelessOp.ExecuteMessage(msg);
            }
            return Task.CompletedTask;
        }

        //If it is barrier message, batch manager will start to track it
        //by using BarrierMsgTrackingInfo which keep and ID and the number of 
        //client it sent to. 
        private Task CheckIfBarrierOrCommitMsg(StreamMessage msg)
        {
            if (msg.Value == barrierMsg.Value)
            {
                currentBatchID = msg.BatchID + 1;
                msg.barrierInfo = new BarrierMsgTrackingInfo(Guid.NewGuid(), onlineMembers.Count);
                PrettyConsole.Line("Send and Start Tracking BatchID: " + msg.BatchID);
                TrackingBarrierMessages(msg);
            }
            else if (msg.Value == commitMsg.Value)
            {
                PrettyConsole.Line("Send comit message for BatchID: " + msg.BatchID);
            }
            else
            {
                msg.BatchID = currentBatchID;
            }
            return Task.CompletedTask;
        }

        private Task TrackingBarrierMessages(StreamMessage msg)
        {
            batchTracker.TrackingBarrierMessages(msg);
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
            return Task.FromResult(batchManager);
        }

        public Task<IBatchTracker> GetBatchTracker()
        {
            return Task.FromResult(batchTracker);
        }

    }
}