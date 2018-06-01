using GrainInterfaces.Model;
using Orleans;
using System.Threading.Tasks;

namespace SystemInterfaces
{
    public interface IStatelessOperator : IGrainWithStringKey
    {
        Task ConsumeMessage(StreamMessage msg);
    }
}
