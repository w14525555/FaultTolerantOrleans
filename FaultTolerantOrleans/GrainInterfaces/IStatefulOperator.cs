using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatefulOperator: IGrainWithGuidKey
    {
        Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream);

        //Method that used for testing. 
        Task<int> GetState(string key);

        Task<int> GetStateInReverseLog(string key);

        Task<int> GetStateInIncrementalLog(string key);

        Task<OperatorSettings> GetSettings();

        Task LoadSettings(OperatorSettings operatorSettings);

        Task SetTracker(IBatchTracker tracker);


    }
}
