using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;
using OrleansFaultTolerance.Util;

namespace OrleansFaultTolerance.Core.Model
{
    public class StreamSource
    {
        private readonly List<StreamMessage> messages = new List<StreamMessage>(100);
        private readonly List<string> onlineMembers = new List<string>(10);
        private readonly StreamMessage barrierMsg = new StreamMessage("System", $"Barrier");
        private readonly StreamMessage commitMsg = new StreamMessage("System", $"Commit");
        private BatchManager batchManager;
        private BatchTracker batchTracker;
        private int currentBatchID;

        private IAsyncStream<StreamMessage> stream;

        public StreamSource(IAsyncStream<StreamMessage> stream, BatchManager batchManager, BatchTracker batchTracker)
        {
            this.stream = stream;
            SetUpBatchManager(batchManager);
            SetUpBatchTracker(batchTracker);
            currentBatchID = 0;
        }

        private Task SetUpBatchManager(BatchManager batchManager)
        {
            this.batchManager = batchManager;
            //batchManager.SetChannelAndRegisterTimer(stream, this);
            return Task.CompletedTask;
        }

        private Task SetUpBatchTracker(BatchTracker batchTracker)
        {
            this.batchTracker = batchTracker;
            batchTracker.SetBatchManager(batchManager);
            return Task.CompletedTask;
        }

        public async Task<Guid> Join(string nickname)
        {
            onlineMembers.Add(nickname);

            await ProduceMessageAsync(new StreamMessage("System", $"{nickname} joins the chat  ..."));

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
            await CheckIfBarrierOrCommitMsg(msg);
            await stream.OnNextAsync(msg);
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

        public Task<BatchManager> GetBatchManager()
        {
            return Task.FromResult(batchManager);
        }

        public Task<BatchTracker> GetBatchTracker()
        {
            return Task.FromResult(batchTracker);
        }
    }
}
