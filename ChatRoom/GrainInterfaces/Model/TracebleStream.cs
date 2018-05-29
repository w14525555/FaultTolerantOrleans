using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GrainInterfaces.Model
{
    public interface TracebleStream
    {
        //This method is used to track the 
        //stream producing messages. 
        Task OnStreamProducing(ChatMsg msg, IAsyncStream<ChatMsg> aStream);
    }
}
