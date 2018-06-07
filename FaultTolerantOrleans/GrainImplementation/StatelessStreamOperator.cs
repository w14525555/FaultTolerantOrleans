using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces;
using SystemInterfaces.Model;

namespace SystemImplementation
{
    class StatelessStreamOperator : Grain, IStatelessOperator
    {
        //The StatelessConsumer does not have state.
        private HashSet<IStatefulOperator> statefulOperators;
        private string downStreamStatefulOne = "statefulOne";
        private string downStreamStatefulTwo = "statefulTwo";

        public override Task OnActivateAsync()
        {
            InitOperators();
            return base.OnActivateAsync();
        }

        private Task InitOperators()
        {
            statefulOperators = new HashSet<IStatefulOperator>();
            statefulOperators.Add(GrainFactory.GetGrain<IStatefulOperator>(downStreamStatefulOne));
            statefulOperators.Add(GrainFactory.GetGrain<IStatefulOperator>(downStreamStatefulTwo));
            return Task.CompletedTask;
        }

        public async Task<Task> ExecuteMessage(StreamMessage msg)
        {
            //At first split text into words
            List<string> words = Utils.Functions.SpiltIntoWords(msg.Value);
            //Then find a operator
            foreach (string word in words)
            {
                IStatefulOperator statefulOperator = await SystemImplementation.PartitionFunction.PartitionStatefulByKey(msg.Key, statefulOperators);
                await statefulOperator.ExecuteMessage(new StreamMessage(word, null));
            }
   
            return Task.CompletedTask;
        }

    }
}
