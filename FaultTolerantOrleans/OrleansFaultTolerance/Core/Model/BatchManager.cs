using OrleansFaultTolerance.Util;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace OrleansFaultTolerance.Core.Model
{
    public class BatchManager
    {
        //A Batch Manager should send batch barrier 
        private readonly StreamMessage barrierMsg = new StreamMessage("System", $"Barrier");
        private readonly StreamMessage commitMsg = new StreamMessage("System", $"Commit");
        //private Boolean isCommitting;
        private StreamSource streamSource;
        private static TimeSpan barrierTimeInterval = TimeSpan.FromSeconds(10);

        private int currentBatchID { get; set; }

        public BatchManager()
        {
            currentBatchID = 0;
            PrettyConsole.Line("Register Timer");
        }

        //public Task SetChannelAndRegisterTimer(IAsyncStream<ChatMsg> stream, IChannel channel)
        //{
        //    RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
        //    this.channel = channel;
        //    return Task.CompletedTask;
        //}

        private async Task<Task> SendBarrierOnPeriodOfTime(object arg)
        {
            await SetBatchID(barrierMsg);
            await streamSource.ProduceMessageAsync(barrierMsg);
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

        public async Task<Task> StartCommit(int ID)
        {
            commitMsg.BatchID = ID;
            await streamSource.ProduceMessageAsync(commitMsg);
            return Task.CompletedTask;
        }
    }
}
