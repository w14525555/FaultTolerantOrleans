using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IOperator: IGrainWithGuidKey
    {
        Task<Task> AddCustomOperators(List<Guid> guidList);

        Task RemoveCustomeOperators(Guid guid);

        Task<int> GetStateInReverseLog(string word);

        Task<int> GetStateInIncrementalLog(string word);

        Task<int> GetState(string word);

        Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream);
    }
}
