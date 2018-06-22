using System;
using System.Collections.Generic;
using System.Linq;

namespace SystemInterfaces.Model
{
    public class Topology
    {
        private Dictionary<Guid, TopologyUnit> topologyUnits = new Dictionary<Guid, TopologyUnit>();

        //This method is used to connect unit
        public void ConnectUnits(Guid upperUnitID, Guid downStreamID)
        {
            var upperStreamUnit = GetUnit(upperUnitID);
            var downStreamUnit = GetUnit(downStreamID);
            downStreamUnit.AddUpperStreamUnit(upperStreamUnit);
            upperStreamUnit.AddDownStreamUnit(downStreamUnit);
        }

        public void DisconnectUnits(Guid upperUnitID, Guid downStreamID)
        {
            var upperStreamUnit = GetUnit(upperUnitID);
            var downStreamUnit = GetUnit(downStreamID);
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

        public int GetSize()
        {
            return topologyUnits.Count;
        }

        public List<TopologyUnit> GetAllTopologyUnits()
        {
            return topologyUnits.Values.ToList();
        }
    }
}
