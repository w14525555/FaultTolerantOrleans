using GrainInterfaces.Interfaces;
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
        private StreamMessage barrierMsg = new StreamMessage(Constants.Barrier_Key, Constants.System_Message_Value);
        private StreamMessage commitMsg = new StreamMessage(Constants.Commit_Key, Constants.System_Message_Value);

        private IStreamSource source;
        private const int Barrier_Time_Interval = 10; 
        private static TimeSpan barrierTimeInterval = TimeSpan.FromSeconds(Barrier_Time_Interval);

        private int currentBatchID { get; set; }

        public override Task OnActivateAsync()
        {
            currentBatchID = 0;
            PrettyConsole.Line("Register Timer");
            return base.OnActivateAsync();
        }

        public Task SetChannelAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource channel)
        {
            RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
            this.source = channel;
            return Task.CompletedTask;
        }

        private Task SendBarrierOnPeriodOfTime(object arg)
        {
            SetBatchID(barrierMsg);
            source.ProduceMessageAsync(barrierMsg);
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
            source.ProduceMessageAsync(commitMsg);
            return Task.CompletedTask;
        }
    }
}
