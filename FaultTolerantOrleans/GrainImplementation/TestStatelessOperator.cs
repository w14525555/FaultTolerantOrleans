using System;
using System.Threading.Tasks;
using Orleans.Streams;
using SystemInterfaces.Model;

namespace SystemImplementation
{
    class TestStatelessOperator : StatelessStreamOperator
    {
        public override Task<Task> CustomExcutionMethod(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            throw new NotImplementedException();
        }
    }
}
