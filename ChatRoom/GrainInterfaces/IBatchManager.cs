using GrainInterfaces.Model;
using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;

namespace GrainInterfaces
{
    public interface IBatchManager: IGrainWithStringKey
    {
        Task SetChannelAndRegisterTimer(IAsyncStream<ChatMsg> stream, IChannel channel);

        Task StartCommit(int ID);
    }
}
