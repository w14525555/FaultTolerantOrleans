using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
	public interface IStreamSource : IGrainWithGuidKey
    {
	    Task<Guid> Join(string nickname);
	    Task<bool> Message(StreamMessage msg);
	    Task<string[]> GetMembers();
        Task<Task> ProduceMessageAsync(StreamMessage msg);
        Task<Task> ReplayTheMessageOnRecoveryCompleted();

        Task<int> GetState(StreamMessage msg);
        Task<int> GetStateInReverseLog(StreamMessage msg);
        Task<int> GetStateInIncrementalLog(StreamMessage msg);
        Task<TopologyUnit> GetTopologyUnit();
        Task Commit(StreamMessage msg);
        Task Recovery(StreamMessage msg);
        Task<int> GetNumberOfElementsInCountMap();
        Task AddCustomDownStreamOperator(IStatelessOperator statelessOperator);
        Task<Guid> GetTestGuid();
        Task<Task> AddCustomDownStreamOperators(List<TopologyUnit> units);
        Task RemoveCustomDownStreamOperator(Guid guid);
        Task InitDeaultOperators();
    }
}
