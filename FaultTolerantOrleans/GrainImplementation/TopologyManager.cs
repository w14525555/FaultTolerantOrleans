using System;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces;
using SystemInterfaces.Model;

namespace SystemImplementation
{
    public class TopologyManager : Grain, ITopology
    {
        private Topology topology = new Topology();

        public Task AddUnit(TopologyUnit unit)
        {
            topology.AddUnit(unit);
            return Task.CompletedTask;
        }

        public Task ConnectUnits(TopologyUnit upperStreamUnit, TopologyUnit downStreamUnit)
        {
            topology.ConnectUnits(upperStreamUnit, downStreamUnit);
            return Task.CompletedTask;
        }

        public Task RemoveUnit(TopologyUnit unit)
        {
            topology.RemoveUnit(unit);
            return Task.CompletedTask;
        }

        public Task UpdateOperatorSettings(Guid guid, OperatorSettings operatorSettings)
        {
            topology.UpdateTopologySettings(guid, operatorSettings);
            return Task.CompletedTask;
        }
    }
}
