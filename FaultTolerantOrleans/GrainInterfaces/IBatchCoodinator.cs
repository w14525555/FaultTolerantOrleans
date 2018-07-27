using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IBatchCoordinator: IGrainWithStringKey
    {
        Task AddSourceAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource channel);

        Task StartCommit(int ID);

        Task<Task> StartRecovery();

        Task<Task> SendBarrier();

        Task CompleteCommit(int batchID);

        Task<Task> CompleteRecovery(int batchID);

        Task<int> GetCommittedBatchID();

        Task StartBarrierTimer();

        Task AddProcessingTime(int time);
    }
}
