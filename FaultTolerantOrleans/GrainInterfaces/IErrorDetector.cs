using Orleans;
using System.Threading.Tasks;

namespace SystemInterfaces
{
    public interface IErrorDetector : IGrainWithStringKey
    {
        Task RegisterTimerToDetectFailures();
    }
}
