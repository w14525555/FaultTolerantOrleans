using System;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface ITopology: IGrainWithStringKey
    {
        Task AddUnit(TopologyUnit unit);

        Task RemoveUnit(Guid key);

        Task ConnectUnits(Guid upperUnitID, Guid downStreamID);

        Task UpdateOperatorSettings(Guid guid, OperatorSettings operatorSettings);

        Task ReplaceTheOldOperatorWithNew(Guid oldGuid, Guid newGuid);

        Task<int> GetTopologySize();

        Task<TopologyUnit> GetUnit(Guid key);

        Task<Task> Commit(StreamMessage msg);

        Task<Task> Recovery(StreamMessage msg);

        Task AddASameTypeStatelessOperatorToTopology(Guid guid);
    }
}
