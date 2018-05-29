using GrainInterfaces.Model;
using Orleans;
using System.Threading.Tasks;

namespace GrainInterfaces
{
    public interface IConsumer: IGrainWithStringKey
    {
        Task ConsumeMessage(ChatMsg msg);

        //Method that used for testing. 
        Task<string> GetState(string key);

        Task<string> GetStateInReverseLog(string key);

        Task<string> GetStateInIncrementalLog(string key);

        Task ClearReverseLog();

        Task UpdateIncrementalLog();

        //For testing
        Task UpdateOperation(ChatMsg msg);

    }
}
