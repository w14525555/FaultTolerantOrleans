using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

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

        public Task ConnectUnits(Guid upperUnitID, Guid downStreamID)
        {
            topology.ConnectUnits(upperUnitID, downStreamID);
            return Task.CompletedTask;
        }

        public Task DisConnectUnits(Guid upperUnitID, Guid downStreamID)
        {
            topology.DisconnectUnits(upperUnitID, downStreamID);
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

            if (oldUnit.operatorType == newUnit.operatorType)
            {
                //If stateful, load the settings and mark the new as a restart grain
                if (newUnit.operatorType == OperatorType.Stateful)
                {
                    IStatefulOperator statefulOp = GrainFactory.GetGrain<IStatefulOperator>(newUnit.primaryKey);
                    statefulOp.LoadSettings(oldUnit.GetSettings());
                }
            }

            //Disconnect the old and connect new
            var upperStreamUnits = oldUnit.GetUpperStreamUnits();
            var downsStreamUnits = oldUnit.GetdownStreamUnits();
            PrettyConsole.Line("Number of upperStream : " + upperStreamUnits.Count);
            PrettyConsole.Line("Number of downStream : " + downsStreamUnits.Count);

            if (upperStreamUnits.Count > 0)
            {
                var keyList = upperStreamUnits.Keys.ToList();
                int index = 0;
                foreach (var item in upperStreamUnits.Values.ToList())
                {
                    DisConnectUnits(item.primaryKey, oldGuid);
                    if (item.operatorType == OperatorType.Stateless)
                    {
                        IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(keyList[index], "SystemImplementation.CountWordStatelessOperator");
                        var guidList = new List<Guid>();
                        guidList.Add(newGuid);
                        statelessOperator.AddCustomeOperators(guidList);
                        statelessOperator.RemoveCustomeOperators(oldGuid);
                    }
                    index++;
                }
            }
            return Task.CompletedTask;
        }

        public Task<int> GetTopologySize()
        {
            return Task.FromResult(topology.GetSize());
        }

        public Task<TopologyUnit> GetUnit(Guid key)
        {
            return Task.FromResult(topology.GetUnit(key));
        }
    } 
}
