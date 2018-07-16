using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;
using System.Linq;

namespace SystemImplementation
{
    public class ErrorDetector: Grain, IErrorDetector
    {
        private IDisposable disposable;
        private TimeSpan detectInterval = TimeSpan.FromSeconds(5);

        public Task RegisterTimerToDetectFailures()
        {
            disposable = RegisterTimer(DetectPossibleFailures, null, TimeSpan.FromSeconds(1), detectInterval);
            return Task.CompletedTask;
        }

        private async Task<Task> DetectPossibleFailures(object org)
        {
            PrettyConsole.Line("Start Detect Failures");
            Dictionary<Guid, Task<int>> taskMap = new Dictionary<Guid, Task<int>>();
            List<Task<int>> taskList = new List<Task<int>>();
            var topologyManager = GrainFactory.GetGrain<ITopology>(Constants.Topology_Manager);
            var operatorUnits = await topologyManager.GetAllUnits();
            foreach (TopologyUnit unit in operatorUnits)
            {
                if (unit.OperatorType == OperatorType.Source)
                {
                    IStreamSource source = GrainFactory.GetGrain<IStreamSource>(unit.PrimaryKey);
                    var task = source.DetectErrors();
                    taskMap.Add(unit.PrimaryKey, task);
                }
                else if (unit.OperatorType == OperatorType.Stateful)
                {
                    IStatefulOperator statefulOperator = GrainFactory.GetGrain<IStatefulOperator>(unit.PrimaryKey, Constants.Stateful_Operator_Prefix);
                    var task = statefulOperator.DetectErrors();
                    taskMap.Add(unit.PrimaryKey, task);
                }
                else if (unit.OperatorType == OperatorType.Stateless)
                {
                    IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(unit.PrimaryKey, Constants.Stateless_Operator_Prefix);
                    var task = statelessOperator.DetectErrors();
                    taskMap.Add(unit.PrimaryKey, task);
                }
                else
                {
                    throw new ArgumentException("Commit: The operator type is in valid!");
                }
            }

            try
            {
                await Task.WhenAny(Task.WhenAll(taskMap.Values), Task.Delay(TimeSpan.FromSeconds(2)));
            }
            catch(Exception e)
            {
                PrettyConsole.Line(e.ToString());
            }

            foreach (var task in taskMap)
            {
                if (task.Value.Status != TaskStatus.RanToCompletion)
                {
                    PrettyConsole.Line("Replace!");
                    topologyManager.ReplaceTheOldOperator(task.Key);
                }
            }

            return Task.CompletedTask;
        }
    }
}
