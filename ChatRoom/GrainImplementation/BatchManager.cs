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
        private readonly ChatMsg barrierMsg = new ChatMsg("System", $"Barrier");
        private readonly ChatMsg commitMsg = new ChatMsg("System", $"Commit");
        //private Boolean isCommitting;
        private IChannel channel;
        private static TimeSpan barrierTimeInterval = TimeSpan.FromSeconds(10);

        private int currentBatchID { get; set; }

        public override Task OnActivateAsync()
        {
            currentBatchID = 0;
            PrettyConsole.Line("Register Timer");
            var streamProvider = GetStreamProvider(Constants.ChatRoomStreamProvider);
            return base.OnActivateAsync();
        }

        public Task SetChannelAndRegisterTimer(IAsyncStream<ChatMsg> stream, IChannel channel)
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

        private Task SetBatchID(ChatMsg msg)
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
