using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

namespace SystemImplementation
{
    public class TopologyManager : Grain, ITopology
    {
        private Topology topology = new Topology();
        private IBatchTracker batchTracker;

        public override Task OnActivateAsync()
        {
            batchTracker = GrainFactory.GetGrain<IBatchTracker>(Constants.Tracker);
            return base.OnActivateAsync();
        }

        public Task AddUnit(TopologyUnit unit)
        {
            topology.AddUnit(unit);
            return Task.CompletedTask;
        }

        public Task ConnectUnits(Guid upperUnitID, Guid downStreamID)
        {
            topology.ConnectUnits(upperUnitID, downStreamID);
            return Task.CompletedTask;
        }

        public Task DisConnectUnits(Guid upperUnitID, Guid downStreamID)
        {
            topology.DisconnectUnits(upperUnitID, downStreamID);
            return Task.CompletedTask;
        }

        public Task RemoveUnit(Guid key)
        {
            topology.RemoveUnit(key);
            return Task.CompletedTask;
        }

        public Task UpdateOperatorSettings(Guid guid, OperatorSettings operatorSettings)
        {
            topology.UpdateTopologySettings(guid, operatorSettings);
            return Task.CompletedTask;
        }

        public Task ReplaceTheOldOperatorWithNew(Guid oldGuid, Guid newGuid)
        {
            var oldUnit = topology.GetUnit(oldGuid);
            var newUnit = topology.GetUnit(newGuid);

            if (oldUnit.operatorType == newUnit.operatorType)
            {
                //If stateful, load the settings and mark the new as a restart grain
                if (newUnit.operatorType == OperatorType.Stateful)
                {
                    IStatefulOperator statefulOp = GrainFactory.GetGrain<IStatefulOperator>(newUnit.primaryKey);
                    statefulOp.LoadSettings(oldUnit.GetSettings());
                }
            }

            //Disconnect the old and connect new
            var upperStreamUnits = oldUnit.GetUpperStreamUnits();
            var downsStreamUnits = oldUnit.GetdownStreamUnits();
            PrettyConsole.Line("Number of upperStream : " + upperStreamUnits.Count);
            PrettyConsole.Line("Number of downStream : " + downsStreamUnits.Count);

            if (upperStreamUnits.Count > 0)
            {
                var keyList = upperStreamUnits.Keys.ToList();
                int index = 0;
                foreach (var item in upperStreamUnits.Values.ToList())
                {
                    DisConnectUnits(item.primaryKey, oldGuid);
                    if (item.operatorType == OperatorType.Stateless)
                    {
                        IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(keyList[index], Constants.Stateless_Operator_Prefix);
                        var guidList = new List<Guid>();
                        guidList.Add(newGuid);
                        statelessOperator.AddCustomeOperators(guidList);
                        statelessOperator.RemoveCustomeOperators(oldGuid);
                    }
                    index++;
                }
            }
            return Task.CompletedTask;
        }

        public Task<int> GetTopologySize()
        {
            return Task.FromResult(topology.GetSize());
        }

        public Task<TopologyUnit> GetUnit(Guid key)
        {
            return Task.FromResult(topology.GetUnit(key));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> Commit(StreamMessage msg)
        {
            List<TopologyUnit> units = topology.GetAllTopologyUnits();
            PrettyConsole.Line("Number of units: " + units.Count);
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), units.Count);
            msg.barrierOrCommitInfo.BatchID = msg.BatchID;
            await batchTracker.TrackingCommitMessages(msg);
            foreach (TopologyUnit unit in units)
            {
                if (unit.operatorType == OperatorType.Source)
                {
                    PrettyConsole.Line("Start Commit Source " + unit.GetSourceKey());
                    IStreamSource source = GrainFactory.GetGrain<IStreamSource>(unit.GetSourceKey());
                    source.Commit(msg);
                }
                else if (unit.operatorType == OperatorType.Stateful)
                {
                    PrettyConsole.Line("Start Commit Stateful");
                    IStatefulOperator statefulOperator = GrainFactory.GetGrain<IStatefulOperator>(unit.primaryKey, Constants.Stateful_Operator_Prefix);
                    statefulOperator.Commit(msg);
                }
                else if (unit.operatorType == OperatorType.Stateless)
                {
                    PrettyConsole.Line("Start Commit Stateless");
                    IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(unit.primaryKey, Constants.Stateless_Operator_Prefix);
                    statelessOperator.Commit(msg);
                }
                else
                {
                    throw new ArgumentException("Commit: The operator type is in valid!");
                }
            }
            return Task.CompletedTask;
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> Recovery(StreamMessage msg)
        {
            List<TopologyUnit> units = topology.GetAllTopologyUnits();
            //PrettyConsole.Line("Number of units: " + units.Count);
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), units.Count);
            msg.barrierOrCommitInfo.BatchID = msg.BatchID;
            await batchTracker.TrackingRecoveryMessages(msg);
            foreach (TopologyUnit unit in units)
            {
                if (unit.operatorType == OperatorType.Source)
                {
                    IStreamSource source = GrainFactory.GetGrain<IStreamSource>(unit.GetSourceKey());
                    source.Recovery(msg);
                }
                else if (unit.operatorType == OperatorType.Stateful)
                {
                    IStatefulOperator statefulOperator = GrainFactory.GetGrain<IStatefulOperator>(unit.primaryKey, Constants.Stateful_Operator_Prefix);
                    statefulOperator.Recovery(msg);
                }
                else if (unit.operatorType == OperatorType.Stateless)
                {
                    IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(unit.primaryKey, Constants.Stateless_Operator_Prefix);
                    statelessOperator.Recovery(msg);
                }
                else
                {
                    throw new ArgumentException("Recovery: The operator type is in valid!");
                }
            }
            return Task.CompletedTask;
        }
    } 
}
