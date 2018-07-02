using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IStatefulOperator: IOperator
    {
        Task<OperatorSettings> GetOperatorSettings();

        Task LoadSettings(OperatorSettings operatorSettings);

        Task MarkOperatorAsFailed();

        Task IncrementNumberOfUpStreamOperator();

        Task DecreseNumberOfUpStreamOperator();

        Task<Task> Commit(StreamMessage msg);

        Task<Task> Recovery(StreamMessage msg);
    }
}
