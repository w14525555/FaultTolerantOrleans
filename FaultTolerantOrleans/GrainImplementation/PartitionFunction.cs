using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemInterfaces;

namespace SystemImplementation
{
    public static class PartitionFunction
    {
        public static int PartitionStatefulByKey(string key, int count)
        {
            int index = Utils.Functions.CalculateHash(key) % count;
            return index;
        }

        public static Task<IOperator> PartitionStatelessByKey(string key, List<IOperator> statelessOperators)
        {
            int index = Utils.Functions.CalculateHash(key) % statelessOperators.Count;
            return Task.FromResult(statelessOperators.ElementAt(index));
        }
    }
}
