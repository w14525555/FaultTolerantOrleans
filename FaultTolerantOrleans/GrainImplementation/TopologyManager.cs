using System;
using System.Collections.Generic;
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

        public Task DisConnectUnits(TopologyUnit upperStreamUnit, TopologyUnit downStreamUnit)
        {
            topology.DisconnectUnits(upperStreamUnit, downStreamUnit);
            return Task.CompletedTask;
        }

        public Task RemoveUnit(Guid key)
        {
            topology.RemoveUnit(key);
            return Task.CompletedTask;
        }

        public Task UpdateOperatorSettings(Guid guid, OperatorSettings operatorSettings)
        {
            topology.UpdateTopologySettings(guid, operatorSettings);
            return Task.CompletedTask;
        }

        public Task ReplaceTheOldOperatorWithNew(Guid oldGuid, Guid newGuid)
        {
            var oldUnit = topology.GetUnit(oldGuid);
            var newUnit = topology.GetUnit(newGuid);

            //Disconnect the old and connect new
            var upperStreamUnits = oldUnit.GetUpperStreamUnits();
            var downsStreamUnits = oldUnit.GetdownStreamUnits();

            if (upperStreamUnits.Count > 0)
            {
                foreach (var item in upperStreamUnits)
                {
                    DisConnectUnits(item.Value, oldUnit);
                    if (item.Value.operatorType == OperatorType.Stateless)
                    {
                        IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(item.Key);
                        var guidList = new List<Guid>();
                        guidList.Add(newGuid);
                        statelessOperator.AddCustomeOperators(guidList);
                        statelessOperator.RemoveCustomeOperators(item.Key);
                    }
                }
            }


            return Task.CompletedTask;
        }
    }
}
