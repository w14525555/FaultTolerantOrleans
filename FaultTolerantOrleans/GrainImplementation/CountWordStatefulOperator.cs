using GrainImplementation;
using Orleans.Streams;
using System.IO;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemImplementation
{
    class CountWordStatefulOperator : StatefulStreamOperator
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public override Task CustomExecutionMethod(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            if (statesMap.ContainsKey(msg.Key))
            {
                //Thrid time throw a exception used for testing recovery
                if (msg.Key == "me" && statesMap[msg.Key] == 2)
                {
                    throw new EndOfStreamException();
                }
                UpdateReverseLog(msg);
                statesMap[msg.Key]++;
                UpdateIncrementalLog(msg);
                stream.OnNextAsync(new StreamMessage(msg.Key, statesMap[msg.Key].ToString()));
            }
            else
            {
                statesMap.Add(msg.Key, 1);
                //If insert, only save the key into reverse log
                reverseLogMap[msg.BatchID].Add(msg.Key, Default_ZERO);
                incrementalLogMap[msg.BatchID].Add(msg.Key, 1);

                stream.OnNextAsync(new StreamMessage(msg.Key, "1"));
            }
            return Task.CompletedTask;
        }
    }
}
