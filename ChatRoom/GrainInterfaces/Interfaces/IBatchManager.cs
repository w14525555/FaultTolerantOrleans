using GrainInterfaces.Model;
using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;

namespace GrainInterfaces.Interfaces
{
    public interface IBatchManager: IGrainWithStringKey
    {
        Task SetChannelAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource channel);

        Task StartCommit(int ID);
    }
}
