using GrainImplementation;
using Orleans.Streams;
using System;
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
            if (CheckStatesMapConstainTheKey(msg.Key))
            {
                //Thrid time throw a exception used for testing recovery
                if (msg.Key == "me" && GetValueFromStatesMap(msg.Key) == 2 && !isARestartOperator)
                {
                    throw new EndOfStreamException();
                }
                UpdateStatesMap(msg, GetValueFromStatesMap(msg.Key)+ 1);
                int processingTime = DateTime.Now.Millisecond - msg.Start_Time;
                batchCoordinator.AddProcessingTime(processingTime);
                stream.OnNextAsync(new StreamMessage(msg.Key, GetValueFromStatesMap(msg.Key).ToString()));
            }
            else
            {
                InsertIntoStatesMap(msg, 1);
                int processingTime = DateTime.Now.Millisecond - msg.Start_Time;
                batchCoordinator.AddProcessingTime(processingTime);
                stream.OnNextAsync(new StreamMessage(msg.Key, "1"));
            }
            return Task.CompletedTask;
        }
    }
}
