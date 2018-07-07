using System;
using System.Collections.Generic;

namespace SystemInterfaces.Model
{
    [Serializable]
    public class IncrementalLog
    {
        public Dictionary<string, object> Log { get; set;}
        public int BatchID { get; set; }
        public LogType logType;
        public IncrementalLog(Dictionary<string, object> Log, int batchID, LogType logType)
        {
            this.Log = Log;
            this.BatchID = batchID;
            this.logType = logType;
        }

    }

    public enum LogType
    {
        Incremental,
        CheckPoint
    }
}
