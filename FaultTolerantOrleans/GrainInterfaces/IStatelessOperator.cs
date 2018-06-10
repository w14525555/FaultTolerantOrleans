using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatelessOperator : IGrainWithGuidKey
    {
        Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream);

        Task SetBatchTracker(IBatchTracker batchTracker);

        Task<int> GetState(string word);

        Task<int> GetStateInReverseLog(string word);

        Task<int> GetStateInIncrementalLog(string word);
    }
}
