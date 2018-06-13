using Orleans;
using Utils;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using SystemInterfaces.Model;
using SystemInterfaces;

namespace GrainImplementation
{
    public class BatchTracker : Grain, IBatchTracker
    {
        private Dictionary<int, StreamBatch> batchTrackingMap = new Dictionary<int, StreamBatch>();
        private Dictionary<int, StreamBatch> commitTrackingMap = new Dictionary<int, StreamBatch>();
        private Dictionary<int, StreamBatch> recoveryTrackingMap = new Dictionary<int, StreamBatch>();

        private List<int> completedBatch = new List<int>();
        private List<int> committedBatch = new List<int>();
        private List<int> recoveryedBatch = new List<int>();
        private IBatchCoordinator batchCoordinator;

        public override Task OnActivateAsync()
        {
            batchCoordinator = GrainFactory.GetGrain<IBatchCoordinator>(Constants.Coordinator);
            return base.OnActivateAsync();
        }

        public Task TrackingBarrierMessages(StreamMessage msg)
        {
            if (batchTrackingMap.ContainsKey(msg.BatchID))
            {
                var targetBatch = batchTrackingMap[msg.BatchID];
                Functions.CheckNotNull(msg.barrierOrCommitInfo);
                targetBatch.AddBarrierOrCommitMsgTrackingHelper(msg.barrierOrCommitInfo);
            }
            else if (msg.BatchID >= 0 && !completedBatch.Contains(msg.BatchID))
            {
                PrettyConsole.Line("Tracking new batch ID " + msg.BatchID);
                StreamBatch newBatch = new StreamBatch(msg.BatchID);
                Functions.CheckNotNull(msg.barrierOrCommitInfo);
                newBatch.AddBarrierOrCommitMsgTrackingHelper(msg.barrierOrCommitInfo);
                batchTrackingMap.Add(msg.BatchID, newBatch);
            }
            else
            {
                throw new InvalidOperationException();
            }

            return Task.CompletedTask;
        }

        //Tracking the commit messages.
        public Task TrackingCommitMessages(StreamMessage msg)
        {
            if (commitTrackingMap.ContainsKey(msg.BatchID))
            {
                var targetBatch = commitTrackingMap[msg.BatchID];
                Functions.CheckNotNull(msg.barrierOrCommitInfo);
                targetBatch.AddBarrierOrCommitMsgTrackingHelper(msg.barrierOrCommitInfo);
            }
            else if (msg.BatchID >= 0 && !committedBatch.Contains(msg.BatchID))
            {
                PrettyConsole.Line("Committing a new batch" + msg.BatchID);
                StreamBatch newBatch = new StreamBatch(msg.BatchID);
                //The name should be changed 
                Functions.CheckNotNull(msg.barrierOrCommitInfo);
                newBatch.AddBarrierOrCommitMsgTrackingHelper(msg.barrierOrCommitInfo);
                commitTrackingMap.Add(msg.BatchID, newBatch);
            }
            else
            {
                throw new InvalidOperationException();
            }

            return Task.CompletedTask;
        }

        //Tracking the Recovery messages.
        //TODO
        public Task TrackingRecoveryMessages(StreamMessage msg)
        {
            if (recoveryTrackingMap.ContainsKey(msg.BatchID))
            {
                var targetBatch = recoveryTrackingMap[msg.BatchID];
                Functions.CheckNotNull(msg.barrierOrCommitInfo);
                targetBatch.AddBarrierOrCommitMsgTrackingHelper(msg.barrierOrCommitInfo);
                throw new InvalidOperationException("Should not recovery different batch at same time");
            }
            else if (recoveryTrackingMap.Count == 1)
            {
                PrettyConsole.Line("Recovery batch" + msg.BatchID);
                StreamBatch newBatch = new StreamBatch(msg.BatchID);
                //The name should be changed 
                Functions.CheckNotNull(msg.barrierOrCommitInfo);
                newBatch.AddBarrierOrCommitMsgTrackingHelper(msg.barrierOrCommitInfo);
                recoveryTrackingMap.Add(msg.BatchID, newBatch);
                return Task.CompletedTask;
            }
            else
            {
                throw new InvalidOperationException("Should not recovery different batch at same time");
            }
        }

        //Should find the target task in the currentBatch
        public Task CompleteOneOperatorBarrier(BarrierOrCommitMsgTrackingInfo msgInfo)
        {
            if (!batchTrackingMap.ContainsKey(msgInfo.BatchID))
            {
                PrettyConsole.Line("The key " + msgInfo.BatchID + " is not exist");
            }
            else
            {
                PrettyConsole.Line("Finish Tracking one message in batchID: " + msgInfo.BatchID);
                StreamBatch targetBatch = batchTrackingMap[msgInfo.BatchID];
                targetBatch.CompleteOneMessageTracking(msgInfo);
                if (targetBatch.readForCommitting)
                {
                    if (batchCoordinator != null)
                    {
                        PrettyConsole.Line("Commit Batch: " + msgInfo.BatchID);
                        SetBatchAsCompleted(msgInfo.BatchID);
                        batchCoordinator.StartCommit(msgInfo.BatchID);
                        batchTrackingMap.Remove(msgInfo.BatchID);
                    }
                }
            }
            return Task.CompletedTask;
        }

        //Should find the target task in the currentBatch
        public async Task<Task> CompleteOneOperatorCommit(BarrierOrCommitMsgTrackingInfo msgInfo)
        {
            if (!commitTrackingMap.ContainsKey(msgInfo.BatchID))
            {
                //Multiple batch has that problem
                PrettyConsole.Line("The commit key " + msgInfo.BatchID + " is not exist");
            }
            else
            {
                //PrettyConsole.Line("Finish Tracking one message in batchID: " + msgInfo.BatchID);
                StreamBatch targetBatch = commitTrackingMap[msgInfo.BatchID];
                targetBatch.CompleteOneMessageTracking(msgInfo);
                if (targetBatch.readForCommitting)
                {
                    if (batchCoordinator != null)
                    {
                        PrettyConsole.Line("Batch: " + msgInfo.BatchID + " commit has been successfully committed");
                        await SetBatchAsCommitted(msgInfo.BatchID);
                        await batchCoordinator.CompleteCommit(msgInfo.BatchID);
                        commitTrackingMap.Remove(msgInfo.BatchID);
                    }
                }
            }
            return Task.CompletedTask;
        }

        //Recovery
        public async Task<Task> CompleteOneOperatorRecovery(BarrierOrCommitMsgTrackingInfo msgInfo)
        {
            if (!recoveryTrackingMap.ContainsKey(msgInfo.BatchID))
            {
                //Multiple batch has that problem
                PrettyConsole.Line("The recovery key " + msgInfo.BatchID + " is not exist");
            }
            else
            {
                //PrettyConsole.Line("Finish Tracking one message in batchID: " + msgInfo.BatchID);
                StreamBatch targetBatch = recoveryTrackingMap[msgInfo.BatchID];
                targetBatch.CompleteOneMessageTracking(msgInfo);
                if (targetBatch.readForCommitting)
                {
                    if (batchCoordinator != null)
                    {
                        PrettyConsole.Line("Batch: " + msgInfo.BatchID + " commit has been successfully recoveryed");
                        recoveryedBatch.Add(msgInfo.BatchID);
                        await batchCoordinator.CompleteRecovery(msgInfo.BatchID);
                        recoveryTrackingMap.Remove(msgInfo.BatchID);
                    }
                }
            }
            return Task.CompletedTask;
        }


        private Task SetBatchAsCompleted(int BatchID)
        {
            if (completedBatch.Contains(BatchID))
            {
                throw new InvalidOperationException();
            }
            else
            {
                completedBatch.Add(BatchID);
            }
            return Task.CompletedTask;
        }

        private Task SetBatchAsCommitted(int BatchID)
        {
            if (committedBatch.Contains(BatchID))
            {
                throw new InvalidOperationException();
            }
            else
            {
                committedBatch.Add(BatchID);
            }
            return Task.CompletedTask;
        }

        //Used For test purpose
        public Task<bool> IsReadyForCommit(int batchID)
        {
            if (completedBatch.Contains(batchID))
            {
                return Task.FromResult(true);
            }
            else
            {
                return Task.FromResult(false);
            }
        }

        public Task<bool> IsCommitSuccess(int batchID)
        {
            if (committedBatch.Contains(batchID))
            {
                return Task.FromResult(true);
            }
            else
            {
                return Task.FromResult(false);
            }
        }

        public Task CleanUpOnRecovery()
        {
            batchTrackingMap.Clear();
            return Task.CompletedTask;
        }

    }



}


