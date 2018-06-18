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

        Task ConnectUnits(TopologyUnit upperStreamUnit, TopologyUnit downStreamUnit);

        Task UpdateOperatorSettings(Guid guid, OperatorSettings operatorSettings);

        Task ReplaceTheOldOperatorWithNew(Guid oldGuid, Guid newGuid);
    }
}
