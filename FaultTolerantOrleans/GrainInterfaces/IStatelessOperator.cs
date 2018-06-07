using Orleans;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatelessOperator : IGrainWithStringKey
    {
        Task<Task> ExecuteMessage(StreamMessage msg);
    }
}
