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
        private List<int> completedBatch = new List<int>();
        private IBatchCoordinator batchManager;

        public Task TrackingBarrierMessages(StreamMessage msg)
        {
            if (batchTrackingMap.ContainsKey(msg.BatchID))
            {
                var targetBatch = batchTrackingMap[msg.BatchID];
                targetBatch.AddBarrierMsgTrackingHelper(msg.barrierInfo);
            }
            else if (msg.BatchID >= 0)
            {
                PrettyConsole.Line("Tracking new batch ID " + msg.BatchID);
                StreamBatch newBatch = new StreamBatch(msg.BatchID);
                newBatch.AddBarrierMsgTrackingHelper(msg.barrierInfo);
                batchTrackingMap.Add(msg.BatchID, newBatch);
            }
            else
            {
                throw new InvalidOperationException();
            }

            return Task.CompletedTask;
        }

        //Should find the target task in the currentBatch
        public Task CompleteTracking(BarrierMsgTrackingInfo msgInfo)
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
                    if (batchManager != null)
                    {
                        PrettyConsole.Line("Commit!");
                        SetBatchAsCompleted(msgInfo.BatchID);
                        //batchManager.StartCommit(msg.BatchID);
                        batchTrackingMap.Remove(msgInfo.BatchID);
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

        public Task SetBatchManager(IBatchCoordinator batchManager)
        {
            this.batchManager = batchManager;
            return Task.CompletedTask;
        }

        //Used For test purpose
        public Task<bool> IsReadForCommit(int batchID)
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

        public Task CleanUpOnRecovery()
        {
            batchTrackingMap.Clear();
            return Task.CompletedTask;
        }

    }



}


