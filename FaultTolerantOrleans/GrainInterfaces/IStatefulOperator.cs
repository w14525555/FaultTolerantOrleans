using Orleans;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatefulOperator: IGrainWithStringKey
    {
        Task ExecuteMessage(StreamMessage msg);

        //Method that used for testing. 
        Task<int> GetState(string key);

        Task<int> GetStateInReverseLog(string key);

        Task<int> GetStateInIncrementalLog(string key);

        Task ClearReverseLog();

        Task<Task> UpdateIncrementalLog();

        //Task RevertStateFromReverseLog();

        Task ReloadStateFromIncrementalLog();

        Task<OperatorSettings> GetSettings();

        Task LoadSettings(OperatorSettings operatorSettings);


    }
}
