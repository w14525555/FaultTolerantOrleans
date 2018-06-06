using Orleans;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatefulOperator: IGrainWithStringKey
    {
        Task ConsumeMessage(StreamMessage msg);

        //Method that used for testing. 
        Task<string> GetState(string key);

        Task<string> GetStateInReverseLog(string key);

        Task<string> GetStateInIncrementalLog(string key);

        Task ClearReverseLog();

        Task<Task> UpdateIncrementalLog();

        Task RevertStateFromReverseLog();

        Task ReloadStateFromIncrementalLog();

        Task<OperatorSettings> GetSettings();

        Task LoadSettings(OperatorSettings operatorSettings);
        //For testing
        Task UpdateOperation(StreamMessage msg);


    }
}
