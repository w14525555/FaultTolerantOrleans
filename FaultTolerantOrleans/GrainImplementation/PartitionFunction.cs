using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemInterfaces;

namespace SystemImplementation
{
    public static class PartitionFunction
    {
        public static Task<IStatefulOperator> PartitionStatefulByKey(string key, HashSet<IStatefulOperator> statefulOperators)
        {
            int index = Utils.Functions.CalculateHash(key) % statefulOperators.Count;
            return Task.FromResult(statefulOperators.ElementAt(index));
        }

        public static Task<IStatelessOperator> PartitionStatelessByKey(string key, HashSet<IStatelessOperator> statelessOperators)
        {
            int index = Utils.Functions.CalculateHash(key) % statelessOperators.Count;
            return Task.FromResult(statelessOperators.ElementAt(index));
        }
    }
}
