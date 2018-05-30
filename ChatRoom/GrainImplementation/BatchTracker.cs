using GrainInterfaces;
using GrainInterfaces.Model;
using Orleans;
using Utils;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;

namespace GrainImplementation
{
    public class BatchTracker : Grain, IBatchTracker
    {
        private Dictionary<int, StreamBatch> batchTrackingMap;
        private IBatchManager batchManager;
        private int initialID;

        public override Task OnActivateAsync()
        {
            //A batch map <BatchID, batch>
            batchTrackingMap = new Dictionary<int, StreamBatch>();
            initialID = 0;
            StreamBatch initialBatch = new StreamBatch(initialID);
            batchTrackingMap.Add(initialID, initialBatch);
            return base.OnActivateAsync();
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
                StreamBatch newBatch = new StreamBatch(msg.BatchID);
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
        public Task CompleteTracking(StreamMessage msg)
        {
            if (!batchTrackingMap.ContainsKey(msg.BatchID))
            {
                throw new InvalidOperationException("The key is not exist");
            }
            else
            {
                PrettyConsole.Line("Finish Tracking batchID: " + msg.BatchID);
                StreamBatch targetBatch = batchTrackingMap[msg.BatchID];
                targetBatch.CompleteOneMessageTracking(msg);
                if (targetBatch.readForCommitting)
                {
                    if (batchManager != null)
                    {
                        batchManager.StartCommit(msg.BatchID);
                        //batchTrackingMap.Remove(msg.BatchID);
                    }
                }
            }
            return Task.CompletedTask;
        }

        public Task SetBatchManager(IBatchManager batchManager)
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


