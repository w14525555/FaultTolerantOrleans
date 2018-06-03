using System;

namespace SystemInterfaces.Model
{
	[Serializable]
	public class StreamMessage
	{
		public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
		public string Key { get; set; } = "Alexey";
		public string Value { get; set; }
        public int BatchID { get; set; }
        public Operation operation { get; set; }
        public BarrierMsgTrackingInfo barrierInfo { get; set; }

		public StreamMessage(string key, string value)
		{
			Key = key;
			Value = value;
        }
    }

    public enum Operation
    {
        Null,
        Update,
        Delete,
        Insert
    }
}