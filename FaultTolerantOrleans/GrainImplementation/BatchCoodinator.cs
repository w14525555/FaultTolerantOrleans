using Orleans;
using Orleans.Streams;
using System;
using System.Threading.Tasks;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

namespace GrainImplementation
{
    public class BatchCoodinator : Grain, IBatchCoordinator
    {
        //A Batch Manager should send batch barrier 
        private StreamMessage barrierMsg = new StreamMessage(Constants.Barrier_Key, Constants.System_Value);
        private StreamMessage commitMsg = new StreamMessage(Constants.Commit_Key, Constants.System_Value);

        private const int Barrier_Interval = 10;
        private TimeSpan barrierTimeInterval = TimeSpan.FromSeconds(Barrier_Interval);

        private IStreamSource source;
       

        private int currentBatchID { get; set; }

        public override Task OnActivateAsync()
        {
            currentBatchID = 0;
            PrettyConsole.Line("Register Timer");
            var streamProvider = GetStreamProvider(Constants.ChatRoomStreamProvider);
            return base.OnActivateAsync();
        }

        public Task SetChannelAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource source)
        {
            RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
            this.source = source;
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

        public Task StartRecovery()
        {
            //TODO
            //1. Stop the timer
            //2. Broadcast the rollback
            //3. Clean information in the tracker
            //4. Reset the batch ID. 
            //5. Make sure everything is right
            //6. Start new batch
            return Task.CompletedTask;
        }
    }
}
