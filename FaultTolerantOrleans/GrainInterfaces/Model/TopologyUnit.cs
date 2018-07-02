using System;
using System.Collections.Generic;
using Utils;

namespace SystemInterfaces.Model
{
    public class TopologyUnit
    {
        private Dictionary<Guid, TopologyUnit> upperStreamUnits = new Dictionary<Guid, TopologyUnit>();
        private Dictionary<Guid, TopologyUnit> downStreamUnits = new Dictionary<Guid, TopologyUnit>();
        private OperatorSettings operatorSettings = new OperatorSettings();
        public OperatorType OperatorType { get; set; }
        public Guid PrimaryKey { get; set; }

        public TopologyUnit(OperatorType operatorType, Guid key)
        {
            this.OperatorType = operatorType;
            PrimaryKey = new Guid();
            PrimaryKey = key;
        }

        public void AddUpperStreamUnit(TopologyUnit unit)
        {
            if (upperStreamUnits.ContainsKey(unit.PrimaryKey))
            {
                throw new ArgumentException("Add Error: The target is already exist in upperStreamUnits!");
            }
            else
            {
                upperStreamUnits.Add(unit.PrimaryKey, unit);
            }
        }

        public void RemoveFromUpperStreamUnits(TopologyUnit unit)
        {
            if (!upperStreamUnits.ContainsKey(unit.PrimaryKey))
            {
                throw new ArgumentException("Remove Error: The target is not exist in upperStreamUnits!");
            }
            else
            {
                upperStreamUnits.Remove(unit.PrimaryKey);
            }
        }

        public void AddDownStreamUnit(TopologyUnit unit)
        {
            if (downStreamUnits.ContainsKey(unit.PrimaryKey))
            {
                throw new ArgumentException("Add Error: The target is already exist in upperStreamUnits!");
            }
            else
            {
                downStreamUnits.Add(unit.PrimaryKey, unit);
            }
        }

        public void RemoveFromDownStreamUnits(TopologyUnit unit)
        {
            if (!downStreamUnits.ContainsKey(unit.PrimaryKey))
            {
                throw new ArgumentException("Remove Error: The target is not exist in upperStreamUnits!");
            }
            else
            {
                downStreamUnits.Remove(unit.PrimaryKey);
            }
        }

        public void UpdateTopologySettings(OperatorSettings operatorSettings)
        {
            this.operatorSettings = operatorSettings;
        }

        public Dictionary<Guid, TopologyUnit> GetUpperStreamUnits()
        {
            return upperStreamUnits;
        }

        public Dictionary<Guid, TopologyUnit> GetdownStreamUnits()
        {
            return downStreamUnits;
        }

        public OperatorSettings GetSettings()
        {
            return operatorSettings;
        }
    }
}
