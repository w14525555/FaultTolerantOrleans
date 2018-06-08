using System;
using System.Collections.Generic;

namespace SystemInterfaces.Model
{
    //This now used for batch manager to track messages. 
    //This should main all the states of certain task for 
    //One batch. Once all the task is finished, it should 
    //send the commit message. 
    public class StreamBatch
    {
        private int batchID;
        public bool readForCommitting { get; set; }
        public bool isAGroupOfMessageProcessed { get; set; }
        private IBatchTracker batchTracker;
        private List<BarrierMsgTrackingInfo> barrierList;

        public StreamBatch(int id)
        {
            readForCommitting = false;
            isAGroupOfMessageProcessed = false;
            batchID = id;
            barrierList = new List<BarrierMsgTrackingInfo>();
        }

        public int GetBatchID()
        {
            return batchID;
        }

        public int GetSize()
        {
            return barrierList.Count;
        }

        public void SetBatchTracker(IBatchTracker batchTracker)
        {
            this.batchTracker = batchTracker;
        }

        public void AddBarrierMsgTrackingHelper(BarrierMsgTrackingInfo barrierInfo)
        {
            barrierList.Add(barrierInfo);
        }

        public void CompleteOneMessageTracking(BarrierMsgTrackingInfo msgInfo)
        {
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
