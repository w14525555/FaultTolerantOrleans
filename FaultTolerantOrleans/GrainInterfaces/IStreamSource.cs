using System;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
	public interface IStreamSource : IGrainWithStringKey
	{
	    Task<Guid> Join(string nickname);
	    Task<Guid> Leave(string nickname);
	    Task<bool> Message(StreamMessage msg);
	    Task<StreamMessage[]> ReadHistory(int numberOfMessages);
	    Task<string[]> GetMembers();
        Task<Task> ProduceMessageAsync(StreamMessage msg);

        Task<Task> ReplayTheMessageOnRecoveryCompleted();
        Task<IBatchCoordinator> GetBatchManager();
        Task<IBatchTracker> GetBatchTracker();
        Task<int> GetState(StreamMessage msg);
        Task<int> GetStateInReverseLog(StreamMessage msg);
        Task<int> GetStateInIncrementalLog(StreamMessage msg);
        Task<TopologyUnit> GetTopologyUnit();
        Task Commit(StreamMessage msg);
        Task Recovery(StreamMessage msg);
        Task<int> GetNumberOfElementsInCountMap();
    }
}
