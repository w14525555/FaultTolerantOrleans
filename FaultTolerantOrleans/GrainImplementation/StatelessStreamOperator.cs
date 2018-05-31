using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GrainInterfaces;
using GrainInterfaces.Model;
using Orleans;

namespace SystemImplementation
{
    class StatelessStreamOperator : Grain, IOperator
    {
        public Task ClearReverseLog()
        {
            throw new NotImplementedException();
        }

        public Task ConsumeMessage(StreamMessage msg)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetState(string key)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetStateInIncrementalLog(string key)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetStateInReverseLog(string key)
        {
            throw new NotImplementedException();
        }

        public Task UpdateIncrementalLog()
        {
            throw new NotImplementedException();
        }

        public Task UpdateOperation(StreamMessage msg)
        {
            throw new NotImplementedException();
        }
    }
}
