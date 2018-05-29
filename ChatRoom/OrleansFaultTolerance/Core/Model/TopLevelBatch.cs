using System;
using System.Collections.Generic;

namespace OrleansFaultTolerance.Core.Model
{
    public class TopLevelBatch
    {
        public int BatchID { get; set;}
        public bool readForCommitting { get; set; }
        public bool isAGroupOfMessageProcessed { get; set; }
        private BatchTracker batchTracker;
        private List<BarrierMsgTrackingInfo> barrierList;

        public TopLevelBatch(int id)
        {
            readForCommitting = false;
            isAGroupOfMessageProcessed = false;
            BatchID = id;
            barrierList = new List<BarrierMsgTrackingInfo>();
        }

        public int GetBatchID()
        {
            return BatchID;
        }

        public int GetSize()
        {
            return barrierList.Count;
        }

        public void SetBatchTracker(BatchTracker batchTracker)
        {
            this.batchTracker = batchTracker;
        }

        public void AddBarrierMsgTrackingHelper(BarrierMsgTrackingInfo barrierInfo)
        {
            barrierList.Add(barrierInfo);
        }

        public void CompleteOneMessageTracking(StreamMessage msg)
        {
            BarrierMsgTrackingInfo msgInfo = msg.barrierInfo;
            foreach (BarrierMsgTrackingInfo item in barrierList)
            {
                if (Guid.Equals(item.GetID(), msgInfo.GetID()))
                {
                    item.CompleteOneMessage();
                    if (item.CheckIfAllMessagesCompleted())
                    {
                        isAGroupOfMessageProcessed = true;
                        CheckIfBatchCompleted();
                    }
                    break;
                }
            }
        }

        public void CheckIfBatchCompleted()
        {
            bool isCompleted = true;
            foreach (BarrierMsgTrackingInfo item in barrierList)
            {
                if (!item.CheckIfAllMessagesCompleted())
                {
                    isCompleted = false;
                }
            }
            if (isCompleted)
            {
                readForCommitting = true;
            }
        }
    }
}
