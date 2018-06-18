using System.Threading.Tasks;

namespace SystemInterfaces.Model
{
    public interface Excutable
    {
        //Task ExecuteMessage();

        Task<TopologyUnit> GetTopologyUnit();
    }
}
