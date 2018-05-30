using OrleansFaultTolerance.Util;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OrleansFaultTolerance.Core.Model
{
    public class BatchTracker
    {
        private Dictionary<int, TopLevelBatch> batchTrackingMap;
        private BatchManager batchManager;
        private int initialID;

        public BatchTracker()
        {
            //A batch map <BatchID, batch>
            batchTrackingMap = new Dictionary<int, TopLevelBatch>();
            initialID = 0;
            TopLevelBatch initialBatch = new TopLevelBatch(initialID);
            batchTrackingMap.Add(initialID, initialBatch);
        }

        public Task TrackingBarrierMessages(StreamMessage msg)
        {
            if (batchTrackingMap.ContainsKey(msg.BatchID))
            {
                var targetBatch = batchTrackingMap[msg.BatchID];
                targetBatch.AddBarrierMsgTrackingHelper(msg.barrierInfo);
            }
            else if (msg.BatchID > 0)
            {
                TopLevelBatch newBatch = new TopLevelBatch(msg.BatchID);
                PrettyConsole.Line("Tracking a new batch!");
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
        public async Task<Task> CompleteTracking(StreamMessage msg)
        {
            if (!batchTrackingMap.ContainsKey(msg.BatchID))
            {
                throw new InvalidOperationException("The key is not exist");
            }
            else
            {
                PrettyConsole.Line("Finish Tracking batchID: " + msg.BatchID);
                TopLevelBatch targetBatch = batchTrackingMap[msg.BatchID];
                targetBatch.CompleteOneMessageTracking(msg);
                if (targetBatch.readForCommitting)
                {
                    if (batchManager != null)
                    {
                        await batchManager.StartCommit(msg.BatchID);
                        //batchTrackingMap.Remove(msg.BatchID);
                    }
                }
            }
            return Task.CompletedTask;
        }

        public Task SetBatchManager(BatchManager batchManager)
        {
            this.batchManager = batchManager;
            return Task.CompletedTask;
        }

        public Task<bool> IsReadForCommit(int batchID)
        {
            if (batchTrackingMap.ContainsKey(batchID))
            {
                return Task.FromResult(batchTrackingMap[batchID].readForCommitting);
            }
            else
            {
                throw new InvalidOperationException();
            }
        }
    }
}
