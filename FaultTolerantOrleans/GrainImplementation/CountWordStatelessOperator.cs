using Orleans.Streams;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemInterfaces;
using SystemInterfaces.Model;

namespace SystemImplementation
{
    public class CountWordStatelessOperator: StatelessStreamOperator
    {
        public override async Task<Task> CustomExcutionMethod(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            List<string> words = Utils.Functions.SpiltIntoWords(msg.Value);
            //Then find a operator
            foreach (string word in words)
            {
                int index = SystemImplementation.PartitionFunction.PartitionStatefulByKey(msg.Key, downStreamOperators.Count);
                IOperator op = downStreamOperators.ElementAt(index);
                StreamMessage newMessage = new StreamMessage(word, null);
                newMessage.BatchID = msg.BatchID;
                await ExecuteMessagesByDownStreamOperators(newMessage, stream, op);
            }
            return Task.CompletedTask;
        }
    }
}
