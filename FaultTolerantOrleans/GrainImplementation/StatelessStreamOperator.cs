using System.Threading.Tasks;
using GrainInterfaces.Model;
using Orleans;
using SystemInterfaces;

namespace SystemImplementation
{
    class StatelessStreamOperator : Grain, IStatelessOperator
    {
        //The StatelessConsumer does not have state. 
        public Task ConsumeMessage(StreamMessage msg)
        {
            throw new System.NotImplementedException();
        }
    }
}
