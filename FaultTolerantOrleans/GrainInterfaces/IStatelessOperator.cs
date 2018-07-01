using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatelessOperator : IOperator
    {
        Task<Task> InitRandomOperators();

        Task Commit(StreamMessage msg);

        Task Recovery(StreamMessage msg);
    }
}
