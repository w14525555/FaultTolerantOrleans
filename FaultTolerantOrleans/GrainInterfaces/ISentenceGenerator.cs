using Orleans;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SystemInterfaces
{
    public interface ISentenceGenerator : IGrainWithStringKey
    {
        Task RegisterTimerAndSetSources(List<IStreamSource> sources);
    }
}
