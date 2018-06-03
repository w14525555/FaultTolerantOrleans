using System.Threading.Tasks;
using Orleans;
using SystemInterfaces;
using SystemInterfaces.Model;

namespace SystemImplementation
{
    class StatelessStreamOperator : Grain, IStatelessOperator
    {
        private int numOfSources;
        //The StatelessConsumer does not have state. 
        public Task ConsumeMessage(StreamMessage msg)
        {
            throw new System.NotImplementedException();
        }

        public Task SetUp(int numOfSources)
        {
            this.numOfSources = numOfSources;
            return Task.CompletedTask;
        }
    }
}
