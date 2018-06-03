using System;

namespace SystemInterfaces.Model
{
    public class BarrierMsgTrackingInfo
    {
        private Guid barrierID;
        private int numberOfClientSent;
        private int numberOfMessagesCompleted;

        public BarrierMsgTrackingInfo(Guid id, int num)
        {
            this.barrierID = id;
            this.numberOfClientSent = num;
            numberOfMessagesCompleted = 0;
        }

        public Guid GetID()
        {
            return barrierID;
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
