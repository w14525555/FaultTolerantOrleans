using Orleans;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    //Once the batch seeder for a batch Bi is emitted by the coordinator,
    //the status of batch Bi in the coordinator is set as
    //Emitting.Meanwhile, the tracker is notified of the newly
    //emitted batch seeder.
    public interface IBatchTracker: IGrainWithStringKey
    {
        Task TrackingBarrierMessages(StreamMessage msg);

        Task CompleteOneOperatorBarrierTracking(BarrierOrCommitMsgTrackingInfo msgInfo);

        Task<bool> IsReadyForCommit(int batchID);

        Task<bool> IsCommitSuccess(int batchID);

        Task CleanUpOnRecovery();

        Task SetBatchManager(IBatchCoordinator batchManager);

        Task TrackingCommitMessages(StreamMessage msg);

        Task<Task> CompleteOneOperatorCommit(BarrierOrCommitMsgTrackingInfo msgInfo);
    }
}
