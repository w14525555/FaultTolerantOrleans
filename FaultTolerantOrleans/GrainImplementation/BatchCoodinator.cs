using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

namespace GrainImplementation
{
    public class BatchCoodinator : Grain, IBatchCoordinator
    {
        //A Batch Manager should send batch barrier 
        private StreamMessage barrierMsg = new StreamMessage(Constants.System_Key, Constants.Barrier_Value);
        private StreamMessage commitMsg = new StreamMessage(Constants.System_Key, Constants.Commit_Value);
        private StreamMessage recoveryMsg = new StreamMessage(Constants.System_Key, Constants.Recovery_Value);

        private const int Barrier_Interval = 10;
        private IDisposable disposable;
        private TimeSpan barrierTimeInterval = TimeSpan.FromSeconds(Barrier_Interval);

        private List<IStreamSource> sources = new List<IStreamSource>();
        private IBatchTracker tracker;
       

        private int currentBatchID { get; set; }
        private int committedID { get; set; }

        public override Task OnActivateAsync()
        {
            currentBatchID = 0;
            committedID = 0;
            PrettyConsole.Line("Register Timer");
            var streamProvider = GetStreamProvider(Constants.ChatRoomStreamProvider);
            return base.OnActivateAsync();
        }

        public Task SetChannelAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource source)
        {
            disposable = RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
            sources.Add(source);
            return Task.CompletedTask;
        }

        private async Task<Task> SendBarrierOnPeriodOfTime(object arg)
        {
            await SetBatchID(barrierMsg);
            foreach (IStreamSource source in sources)
            {
                await source.ProduceMessageAsync(barrierMsg);
            }
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

        //Commit 
        public Task StartCommit(int ID)
        {
            commitMsg.BatchID = ID;
            committedID = ID;
            foreach (IStreamSource source in sources)
            {
                source.ProduceMessageAsync(commitMsg);
            }
            return Task.CompletedTask;
        }

        public Task SetTracker(IBatchTracker tracker)
        {
            this.tracker = tracker;
            return Task.CompletedTask;
        }

        public Task StartRecovery()
        {
            //TODO
            //1. Stop the timer
            disposable.Dispose();
            //2. Broadcast the rollback and reset batchID
            recoveryMsg.Value = committedID.ToString();
            //source.ProduceMessageAsync(recoveryMsg);
            //3. Clean information in the tracker()
            tracker.CleanUpOnRecovery();
            //5. Make sure everything is right
            //6. Register new timer
            disposable = RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
            return Task.CompletedTask;
        }
    }
}
