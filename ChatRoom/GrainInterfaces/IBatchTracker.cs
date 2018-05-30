using GrainInterfaces.Model;
using Orleans;
using System.Threading.Tasks;

namespace GrainInterfaces
{
    //Once the batch seeder for a batch Bi is emitted by the coordinator,
    //the status of batch Bi in the coordinator is set as
    //Emitting.Meanwhile, the tracker is notified of the newly
    //emitted batch seeder.
    public interface IBatchTracker: IGrainWithStringKey
    {
        Task TrackingBarrierMessages(StreamMessage msg);

        Task CompleteTracking(StreamMessage msg);

        Task<bool> IsReadForCommit(int batchID);

        Task SetBatchManager(IBatchManager batchManager);
    }
}
