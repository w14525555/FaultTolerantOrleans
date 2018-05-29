using System;

namespace GrainInterfaces.Model
{
	[Serializable]
	public class ChatMsg
	{
		public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
		public string Author { get; set; } = "Alexey";
		public string Text { get; set; }
        public int BatchID { get; set; }
        public Operation operation { get; set; }
        public BarrierMsgTrackingInfo barrierInfo { get; set; }

		public ChatMsg(string author, string msg)
		{
			Author = author;
			Text = msg;
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