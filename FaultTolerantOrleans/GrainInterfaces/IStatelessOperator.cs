using Orleans;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatelessOperator : IGrainWithStringKey
    {
        Task ConsumeMessage(StreamMessage msg);

        Task SetUp(int numOfSources);
    }
}
