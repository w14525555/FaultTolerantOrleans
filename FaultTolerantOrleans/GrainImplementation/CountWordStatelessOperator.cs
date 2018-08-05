using Orleans.Streams;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

namespace SystemImplementation
{
    public class CountWordStatelessOperator: StatelessStreamOperator
    {
        private int roundRobinValue = 0;

        public override async Task<Task> CustomExcutionMethod(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            List<string> words = Utils.Functions.SpiltIntoWords(msg.Value);
            //Then find a operator
            foreach (string word in words)
            {
                //int index = SystemImplementation.PartitionFunction.PartitionOperatorByKey(msg.Key, downStreamOperators.Count);
                int index = roundRobinValue % downStreamOperators.Count;
                roundRobinValue++;
                IOperator op = downStreamOperators.ElementAt(index);
                //PrettyConsole.Line(index.ToString());
                StreamMessage newMessage = new StreamMessage(word, null);
                newMessage.Start_Time = msg.Start_Time;
                newMessage.BatchID = msg.BatchID;
                await ExecuteMessagesByDownStreamOperators(newMessage, stream, op);
            }
            return Task.CompletedTask;
        }
    }
}
