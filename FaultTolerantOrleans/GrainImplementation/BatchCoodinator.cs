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
            committedID = -1;
            tracker = GrainFactory.GetGrain<IBatchTracker>(Utils.Constants.Tracker);
            PrettyConsole.Line("Register Timer");
            var streamProvider = GetStreamProvider(Constants.FaultTolerantStreamProvider);
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
            await SendBarrier();
            return Task.CompletedTask;
        }

        public async Task<Task> SendBarrier()
        {
            barrierMsg.BatchID = currentBatchID;
            barrierMsg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), sources.Count);
            await tracker.TrackingBarrierMessages(barrierMsg);
            foreach (IStreamSource source in sources)
            {
                await source.ProduceMessageAsync(barrierMsg);
            }
            currentBatchID++;
            return Task.CompletedTask;
        }

        //Commit 
        public async Task<Task> StartCommit(int ID)
        {
            commitMsg.BatchID = ID;
            commitMsg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), sources.Count);
            await tracker.TrackingCommitMessages(commitMsg);
            foreach (IStreamSource source in sources)
            {
                await source.ProduceMessageAsync(commitMsg);
            }
            return Task.CompletedTask;
        }

        //Recovery
        public Task StartRecovery()
        {
            //1. Stop the timer
            disposable.Dispose();
            //2. Broadcast the rollback and reset batchID
            recoveryMsg.BatchID = committedID;
            recoveryMsg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), sources.Count);
            tracker.TrackingRecoveryMessages(recoveryMsg);
            foreach (IStreamSource source in sources)
            {
                source.ProduceMessageAsync(recoveryMsg);
            }
            //3. Clean information in the tracker()
            tracker.CleanUpOnRecovery();
            //6. Register new timer
            return Task.CompletedTask;
        }

        //Once the recovery completed, just restart the timer
        //Restart the timer
        public Task CompleteRecovery(int batchID)
        {
            if (committedID == batchID)
            {
                currentBatchID = batchID + 1;
                disposable = RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
                //ReplayTheMessagesOnRecoveryCompleted();
                return Task.CompletedTask;
            }
            else
            {
                throw new InvalidOperationException("The recvoery batch is not equal to the latest committed ID");
            }
        }

        public Task ReplayTheMessagesOnRecoveryCompleted()
        {
            foreach(var source in sources)
            {
                source.ReplayTheMessageOnRecoveryCompleted();
            }
            return Task.CompletedTask;
        }

        public Task SetCurrentBatchID(int id)
        {
            currentBatchID = id;
            return Task.CompletedTask;
        }

        public Task CompleteCommit(int batchID)
        {
            if (batchID - committedID == 1)
            {
                committedID++;
                PrettyConsole.Line("Committed Batch ID now is: " + committedID);
                return Task.CompletedTask;
            }
            else
            {
                throw new InvalidOperationException("Cannot commit batch greater than committed id 2");
            }
        }

        public Task<int> GetCommittedBatchID()
        {
            return Task.FromResult(committedID);
        }
    }
}
