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
        private Dictionary<int, StreamBatch> batchTrackingMap;
        private IBatchCoordinator batchManager;

        public override Task OnActivateAsync()
        {
            //A batch map <BatchID, batch>
            batchTrackingMap = new Dictionary<int, StreamBatch>();
            return base.OnActivateAsync();
        }

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
                        //batchManager.StartCommit(msg.BatchID);
                        batchTrackingMap.Remove(msgInfo.BatchID);
                    }
                }
            }
            return Task.CompletedTask;
        }

        public Task SetBatchManager(IBatchCoordinator batchManager)
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

        public Task CleanUpOnRecovery()
        {
            batchTrackingMap.Clear();
            return Task.CompletedTask;
        }

    }



}


