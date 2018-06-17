using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SystemInterfaces.Model
{
    public class TopologyUnit
    {
        private Dictionary<Guid, TopologyUnit> upperStreamUnits = new Dictionary<Guid, TopologyUnit>();
        private Dictionary<Guid, TopologyUnit> downStreamUnits = new Dictionary<Guid, TopologyUnit>();
        private OperatorSettings operatorSettings = new OperatorSettings();
        private OperatorType operatorType;
        public Guid primaryKey { get; set; }

        public TopologyUnit(OperatorType operatorType, Guid key)
        {
            this.operatorType = operatorType;
            primaryKey = key;
        }

        public void AddUpperStreamUnit(TopologyUnit unit)
        {
            if (upperStreamUnits.ContainsKey(unit.primaryKey))
            {
                throw new ArgumentException("Add Error: The target is already exist in upperStreamUnits!");
            }
            else
            {
                upperStreamUnits.Add(unit.primaryKey, unit);
            }
        }

        public void RemoveFromUpperStreamUnits(TopologyUnit unit)
        {
            if (!upperStreamUnits.ContainsKey(unit.primaryKey))
            {
                throw new ArgumentException("Remove Error: The target is not exist in upperStreamUnits!");
            }
            else
            {
                upperStreamUnits.Remove(unit.primaryKey);
            }
        }

        public void AddDownStreamUnit(TopologyUnit unit)
        {
            if (downStreamUnits.ContainsKey(unit.primaryKey))
            {
                throw new ArgumentException("Add Error: The target is already exist in upperStreamUnits!");
            }
            else
            {
                downStreamUnits.Add(unit.primaryKey, unit);
            }
        }

        public void RemoveFromDownStreamUnits(TopologyUnit unit)
        {
            if (!downStreamUnits.ContainsKey(unit.primaryKey))
            {
                throw new ArgumentException("Remove Error: The target is not exist in upperStreamUnits!");
            }
            else
            {
                downStreamUnits.Remove(unit.primaryKey);
            }
        }

    }
}
