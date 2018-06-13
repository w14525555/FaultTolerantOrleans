
using System;
using System.Collections.Generic;

namespace SystemInterfaces.Model
{
    public class OperatorSettings
    {
        public string incrementalLogAddress { get; set; }
        public OperatorType operatorType { get; set; }
        private Dictionary<Guid, OperatorSettings> operatorDict;

        public OperatorSettings()
        {
            operatorDict = new Dictionary<Guid, OperatorSettings>();
        }

        public void AddOpratorToDict(Guid guid, OperatorSettings operatorSettings)
        {
            if (!operatorDict.ContainsKey(guid))
            {
                operatorDict.Add(guid, operatorSettings);
            }
            else
            {
                throw new ArgumentException("The guid is exist!");
            }
        }

        public void RemoveOperatorFromDict(Guid guid)
        {
            if (operatorDict.ContainsKey(guid))
            {
                operatorDict.Remove(guid);
            }
            else
            {
                throw new InvalidOperationException("Try to remove the non-exist key");
            }
        }

        public Dictionary<Guid, OperatorSettings> GetOperatorDict()
        {
            return operatorDict;
        }
    }

    public enum OperatorType
    {
        Null,
        Stateful,
        Stateless,
        Source
    }
}
