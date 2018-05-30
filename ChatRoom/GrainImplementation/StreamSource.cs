using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GrainInterfaces;
using GrainInterfaces.Model;
using Orleans;
using Orleans.Streams;
using Utils;

namespace GrainImplementation
{
	public class StreamSource : Grain, IChannel
	{
		private readonly List<ChatMsg> messages = new List<ChatMsg>(100);
		private readonly List<string> onlineMembers = new List<string>(10);
        private readonly ChatMsg barrierMsg = new ChatMsg("System", $"Barrier");
        private readonly ChatMsg commitMsg = new ChatMsg("System", $"Commit");
        private IBatchManager batchManager;
        private IBatchTracker batchTracker;
        private int currentBatchID;

		private IAsyncStream<ChatMsg> stream;

		public override Task OnActivateAsync()
		{
			var streamProvider = GetStreamProvider(Constants.ChatRoomStreamProvider);
            stream = streamProvider.GetStream<ChatMsg>(Guid.NewGuid(), Constants.CharRoomStreamNameSpace);
            SetUpBatchManager();
            SetUpBatchTracker();
            currentBatchID = 0;
            return base.OnActivateAsync();
		}

        private Task SetUpBatchManager()
        {
            batchManager = GrainFactory.GetGrain<IBatchManager>("Manager");
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

            await ProduceMessageAsync(new ChatMsg("System", $"{nickname} joins the chat '{this.GetPrimaryKeyString()}' ..."));

			return stream.Guid;
		}

		public async Task<Guid> Leave(string nickname)
		{
			onlineMembers.Remove(nickname);

            await ProduceMessageAsync(new ChatMsg("System", $"{nickname} leaves the chat..."));

			return stream.Guid;
		}

		public async Task<bool> Message(ChatMsg msg)
		{
			messages.Add(msg);
            await ProduceMessageAsync(msg);
			return true;
		}

        //This method has to be async because you have to wait
        //the stream sends messages to all its subscribers
         public async Task<Task> ProduceMessageAsync(ChatMsg msg)
        {
            await CheckIfBarrierOrCommitMsg(msg);
            await stream.OnNextAsync(msg);
            return Task.CompletedTask;
        }

        //If it is barrier message, batch manager will start to track it
        //by using BarrierMsgTrackingInfo which keep and ID and the number of 
        //client it sent to. 
        private Task CheckIfBarrierOrCommitMsg(ChatMsg msg)
        {
            if (msg.Text == barrierMsg.Text)
            {
                currentBatchID = msg.BatchID + 1;
                msg.barrierInfo = new BarrierMsgTrackingInfo(Guid.NewGuid(), onlineMembers.Count);
                PrettyConsole.Line("Send and Start Tracking BatchID: " + msg.BatchID);
                TrackingBarrierMessages(msg);
            }
            else if (msg.Text == commitMsg.Text)
            {
                PrettyConsole.Line("Send comit message for BatchID: " + msg.BatchID);
            }
            else
            {
                msg.BatchID = currentBatchID;
            }
            return Task.CompletedTask;
        }

        private Task TrackingBarrierMessages(ChatMsg msg)
        {
            batchTracker.TrackingBarrierMessages(msg);
            return Task.CompletedTask;
        }


        public Task<string[]> GetMembers()
	    {
	        return Task.FromResult(onlineMembers.ToArray());
	    }

	    public Task<ChatMsg[]> ReadHistory(int numberOfMessages)
	    {
	        var response = messages
	            .OrderByDescending(x => x.Created)
	            .Take(numberOfMessages)
	            .OrderBy(x => x.Created)
	            .ToArray();

	        return Task.FromResult(response);
	    }

        public Task<IBatchManager> GetBatchManager()
        {
            return Task.FromResult(batchManager);
        }

        public Task<IBatchTracker> GetBatchTracker()
        {
            return Task.FromResult(batchTracker);
        }

    }
}