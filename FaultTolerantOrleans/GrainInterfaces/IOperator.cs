using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SystemInterfaces
{
    public interface IOperator: IGrainWithGuidKey
    {
        Task<Task> AddCustomOperators(List<Guid> guidList);
        Task RemoveCustomeOperators(Guid guid);
    }
}
