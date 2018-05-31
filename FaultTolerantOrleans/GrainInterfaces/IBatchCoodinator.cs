using GrainInterfaces.Model;
using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;

namespace GrainInterfaces
{
    public interface IBatchCoordinator: IGrainWithStringKey
    {
        Task SetChannelAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource channel);

        Task StartCommit(int ID);
    }
}
