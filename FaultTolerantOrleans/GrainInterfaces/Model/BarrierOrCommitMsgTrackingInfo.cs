using System;

namespace SystemInterfaces.Model
{
    public class BarrierOrCommitMsgTrackingInfo
    {
        private Guid guid;
        public int BatchID { get; set; }
        public int numberOfClientSent { get; set; }
        private int numberOfMessagesCompleted;

        public BarrierOrCommitMsgTrackingInfo(Guid id, int num)
        {
            this.guid = id;
            this.numberOfClientSent = num;
            numberOfMessagesCompleted = 0;
        }

        public Guid GetID()
        {
            return guid;
        }

        public void CompleteOneMessage()
        {
            numberOfMessagesCompleted++;

            if (numberOfMessagesCompleted > numberOfClientSent)
            {
                throw new InvalidOperationException();
            }
        }

        public bool CheckIfAllMessagesCompleted()
        {
            return numberOfClientSent == numberOfMessagesCompleted;
        }
    }
}
