using System;
using System.Collections.Generic;

namespace SystemInterfaces.Model
{
    public class Topology
    {
        private Dictionary<Guid, TopologyUnit> topologyUnits = new Dictionary<Guid, TopologyUnit>();

        //This method is used to connect unit
        public void ConnectUnits(TopologyUnit upperStreamUnit, TopologyUnit downStreamUnit)
        {
            upperStreamUnit.AddDownStreamUnit(downStreamUnit);
            downStreamUnit.AddUpperStreamUnit(upperStreamUnit);
        }

        public void DisconnectUnits(TopologyUnit upperStreamUnit, TopologyUnit downStreamUnit)
        {
            upperStreamUnit.RemoveFromDownStreamUnits(downStreamUnit);
            downStreamUnit.RemoveFromUpperStreamUnits(upperStreamUnit);
        }

        public void AddUnit(TopologyUnit unit)
        {
            if (topologyUnits.ContainsKey(unit.primaryKey))
            {
                throw new ArgumentException("Add Error: The target is already exist in upperStreamUnits!");
            }
            else
            {
                topologyUnits.Add(unit.primaryKey, unit);
            }
        }

        public void RemoveUnit(Guid key)
        {
            if (!topologyUnits.ContainsKey(key))
            {
                throw new ArgumentException("Remove Error: The target is not exist in upperStreamUnits!");
            }
            else
            {
                topologyUnits.Remove(key);
            }
        }

        public void UpdateTopologySettings(Guid guid, OperatorSettings operatorSettings)
        {
            if (topologyUnits.ContainsKey(guid))
            {
                topologyUnits[guid].UpdateTopologySettings(operatorSettings);
            }
            else
            {
                throw new ArgumentException("Remove Error: The target is not exist in upperStreamUnits!");
            }
        }

        public TopologyUnit GetUnit(Guid key)
        {
            if (topologyUnits.ContainsKey(key))
            {
                return topologyUnits[key];
            }
            else
            {
                throw new ArgumentException("Get Error: the target is not exist");
            }
        }

    }
}
