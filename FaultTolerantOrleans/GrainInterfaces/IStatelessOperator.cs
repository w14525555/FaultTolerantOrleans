using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatelessOperator : IGrainWithGuidKey
    {
        Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream);

        Task<int> GetState(string word);

        Task<int> GetStateInReverseLog(string word);

        Task<int> GetStateInIncrementalLog(string word);

        Task<Task> InitRandomOperators();

        Task<Task> InitCustomerOperators(List<Guid> guidList);

        Task<TopologyUnit> GetTopologyUnit();
    }
}
