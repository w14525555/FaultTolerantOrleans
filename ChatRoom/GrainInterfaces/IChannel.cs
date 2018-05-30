using System;
using System.Threading.Tasks;
using GrainInterfaces.Model;
using Orleans;

namespace GrainInterfaces
{
	public interface IChannel : IGrainWithStringKey
	{
	    Task<Guid> Join(string nickname);
	    Task<Guid> Leave(string nickname);
	    Task<bool> Message(StreamMessage msg);
	    Task<StreamMessage[]> ReadHistory(int numberOfMessages);
	    Task<string[]> GetMembers();
        Task<Task> ProduceMessageAsync(StreamMessage msg);
        //Use for testing purpose
        Task<IBatchManager> GetBatchManager();
        Task<IBatchTracker> GetBatchTracker();
        

    }
}
