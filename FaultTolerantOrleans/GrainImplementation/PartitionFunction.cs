using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SystemInterfaces;

namespace SystemImplementation
{
    public static class PartitionFunction
    {
        public static int PartitionOperatorByKey(string key, int count)
        {
            int index = Utils.Functions.CalculateHash(key) % count;
            return index;
        }
    }
}
