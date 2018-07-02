using System;
using System.Collections.Generic;

namespace SystemInterfaces.Model
{
    [Serializable]
    public class IncrementalLog
    {
        public Dictionary<string, int> Log { get; set;}
        public int BatchID { get; set; }
        public IncrementalLog(Dictionary<string, int> Log, int batchID)
        {
            this.Log = Log;
            this.BatchID = batchID;
        }

    }
}
