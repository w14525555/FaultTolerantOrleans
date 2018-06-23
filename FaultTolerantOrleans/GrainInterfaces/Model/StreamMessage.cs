using System;

namespace SystemInterfaces.Model
{
	[Serializable]
	public class StreamMessage
	{
        public MessageType messageType { get; set; }
        public BarrierOrCommitMsgTrackingInfo barrierOrCommitInfo { get; set; }
        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
        public Guid From { get; set; }
        public int Count { get; set; }
        public StreamValue streamValue { get; set; }
        public string Key { get; set; } = "Alexey";
		public string Value { get; set; }
        public int BatchID { get; set; }

		public StreamMessage(string key, string value)
		{
			Key = key;
			Value = value;
            streamValue = new StreamValue();
            streamValue.innerValue = value;
        }
    }

    public enum MessageType
    {
        Null,
        Test,
    }
}