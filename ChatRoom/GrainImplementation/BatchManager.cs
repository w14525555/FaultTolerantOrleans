using GrainInterfaces;
using GrainInterfaces.Model;
using Orleans;
using Orleans.Streams;
using System;
using System.Threading.Tasks;
using Utils;

namespace GrainImplementation
{
    public class BatchManager : Grain, IBatchManager
    {
        //A Batch Manager should send batch barrier 
        private StreamMessage barrierMsg = new StreamMessage(Constants.Barrier_Key, Constants.System_Value);
        private StreamMessage commitMsg = new StreamMessage(Constants.Commit_Key, Constants.System_Value);
        //private Boolean isCommitting;
        private IStreamSource channel;
        private static TimeSpan barrierTimeInterval = TimeSpan.FromSeconds(10);

        private int currentBatchID { get; set; }

        public override Task OnActivateAsync()
        {
            currentBatchID = 0;
            PrettyConsole.Line("Register Timer");
            var streamProvider = GetStreamProvider(Constants.ChatRoomStreamProvider);
            return base.OnActivateAsync();
        }

        public Task SetChannelAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource channel)
        {
            RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
            this.channel = channel;
            return Task.CompletedTask;
        }

        private Task SendBarrierOnPeriodOfTime(object arg)
        {
            SetBatchID(barrierMsg);
            channel.ProduceMessageAsync(barrierMsg);
            currentBatchID++;
            return Task.CompletedTask;
        }

        private Task SetBatchID(StreamMessage msg)
        {
            msg.BatchID = currentBatchID;
            return Task.CompletedTask;
        }

        public Task SetCurrentBatchID(int id)
        {
            currentBatchID = id;
            return Task.CompletedTask;
        }

        public Task StartCommit(int ID)
        {
            commitMsg.BatchID = ID;
            channel.ProduceMessageAsync(commitMsg);
            return Task.CompletedTask;
        }
    }
}
