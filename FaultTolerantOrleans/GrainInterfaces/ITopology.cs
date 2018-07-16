using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface ITopology: IGrainWithStringKey
    {
        Task AddUnit(TopologyUnit unit);

        Task RemoveUnit(Guid key);

        Task<List<TopologyUnit>> GetAllUnits();

        Task ConnectUnits(Guid upperUnitID, Guid downStreamID);

        Task UpdateOperatorSettings(Guid guid, OperatorSettings operatorSettings);

        Task<Task> ReplaceTheOldOperator(Guid oldGuid);

        Task<int> GetTopologySize();

        Task<TopologyUnit> GetUnit(Guid key);

        Task<Task> Commit(StreamMessage msg);

        Task<Task> Recovery(StreamMessage msg);

        Task AddASameTypeStatelessOperatorToTopology(Guid guid);

        Task<List<IStreamSource>> GetRandomSources(int num);

        Task<List<IOperator>> GetRandomStatefulOperators(int num);

        Task<List<IOperator>> GetRandomStatelessOperators(int num);

        Task<Task> AddCustomeOperatorsToNonSourceOperators(List<IOperator> ops, List<IOperator> operators);

        Task<Task> AddCustomeOperatorsToSources(List<IStreamSource> sources, List<IOperator> operators);
    }
}
