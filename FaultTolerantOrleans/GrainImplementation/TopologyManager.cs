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

            if (oldUnit.OperatorType == newUnit.OperatorType)
            {
                //Only the stateful load the settings
                if (newUnit.OperatorType == OperatorType.Stateful)
                {
                    IStatefulOperator statefulOp = GrainFactory.GetGrain<IStatefulOperator>(newUnit.PrimaryKey, Constants.Stateful_Operator_Prefix);
                    statefulOp.LoadSettings(oldUnit.GetSettings());
                }
            }

            //Disconnect the old and connect new
            var upperStreamUnits = oldUnit.GetUpperStreamUnits();
            var downsStreamUnits = oldUnit.GetdownStreamUnits();
            PrettyConsole.Line("Number of upperStream : " + upperStreamUnits.Count);
            PrettyConsole.Line("Number of downStream : " + downsStreamUnits.Count);

            //Handle Upper Stream
            if (upperStreamUnits.Count > 0)
            {
                var keyList = upperStreamUnits.Keys.ToList();
                int index = 0;
                foreach (var item in upperStreamUnits.Values.ToList())
                {
                    DisConnectUnits(item.PrimaryKey, oldGuid);
                    IOperator op;
                    var unitList = new List<TopologyUnit>();
                    unitList.Add(newUnit);

                    if (item.OperatorType == OperatorType.Stateless)
                    {
                        op = GrainFactory.GetGrain<IOperator>(keyList[index], Constants.Stateless_Operator_Prefix);
                        op.AddCustomDownStreamOperators(unitList);
                        op.RemoveCustomDownStreamOperator(oldGuid);
                    }
                    else if (item.OperatorType == OperatorType.Stateful)
                    {
                        op = GrainFactory.GetGrain<IOperator>(keyList[index], Constants.Stateful_Operator_Prefix);
                        op.AddCustomDownStreamOperators(unitList);
                        op.RemoveCustomDownStreamOperator(oldGuid);
                    }
                    else if (item.OperatorType == OperatorType.Source)
                    {
                        var source = GrainFactory.GetGrain<IStreamSource>(item.PrimaryKey);
                        source.AddCustomDownStreamOperators(unitList);
                        source.RemoveCustomDownStreamOperator(oldGuid);
                    }
                    index++;
                }
            }

            //Handle Down Stream
            if (downsStreamUnits.Count > 0)
            {
                var units = downsStreamUnits.Values.ToList();
                IOperator newOp;
                if (newUnit.OperatorType == OperatorType.Stateless)
                {
                    newOp = GrainFactory.GetGrain<IStatelessOperator>(newGuid, Constants.Stateless_Operator_Prefix);
                    newOp.AddCustomDownStreamOperators(units);
                }
                else if (newUnit.OperatorType == OperatorType.Stateful)
                {
                    newOp = GrainFactory.GetGrain<IStatefulOperator>(newGuid, Constants.Stateful_Operator_Prefix);
                    newOp.AddCustomDownStreamOperators(units);
                }
                else
                {
                    throw new ArgumentException("Source cannot be down stream operator");
                }
            }
            return Task.CompletedTask;
        }



        public Task AddASameTypeStatelessOperatorToTopology(Guid guid)
        {
            var unit = topology.GetUnit(guid);
            if (unit.OperatorType == OperatorType.Stateless)
            {
                var newUnit = new TopologyUnit(OperatorType.Stateless, Guid.NewGuid());
                //To Add a new operator, we need connect it with all the upper stream units 
                //and lower stream unit
                var upperStreamUnits = unit.GetUpperStreamUnits();
                var downsStreamUnits = unit.GetdownStreamUnits();

                var newStatelessOp = GrainFactory.GetGrain<IStatelessOperator>(newUnit.PrimaryKey, Constants.Stateless_Operator_Prefix);
                
                //Add down stream by this unit
                newStatelessOp.AddCustomDownStreamOperators(downsStreamUnits.Values.ToList());

                foreach (var op in upperStreamUnits)
                {
                    if (op.Value.OperatorType == OperatorType.Source)
                    {
                        var source = GrainFactory.GetGrain<IStreamSource>(op.Key);
                        source.AddCustomDownStreamOperator(newStatelessOp);
                    }
                }

            }
            else
            {
                throw new ArgumentException("The guid is not a stateless operator!");
            }
            return Task.CompletedTask;
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
                if (unit.OperatorType == OperatorType.Source)
                {
                    IStreamSource source = GrainFactory.GetGrain<IStreamSource>(unit.PrimaryKey);
                    source.Commit(msg);
                }
                else if (unit.OperatorType == OperatorType.Stateful)
                {
                    IStatefulOperator statefulOperator = GrainFactory.GetGrain<IStatefulOperator>(unit.PrimaryKey, Constants.Stateful_Operator_Prefix);
                    statefulOperator.Commit(msg);
                }
                else if (unit.OperatorType == OperatorType.Stateless)
                {
                    IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(unit.PrimaryKey, Constants.Stateless_Operator_Prefix);
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
            msg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), units.Count);
            msg.barrierOrCommitInfo.BatchID = msg.BatchID;
            await batchTracker.TrackingRecoveryMessages(msg);
            foreach (TopologyUnit unit in units)
            {
                if (unit.OperatorType == OperatorType.Source)
                {
                    IStreamSource source = GrainFactory.GetGrain<IStreamSource>(unit.PrimaryKey);
                    source.Recovery(msg);
                }
                else if (unit.OperatorType == OperatorType.Stateful)
                {
                    PrettyConsole.Line("Recovery a stateful");
                    IStatefulOperator statefulOperator = GrainFactory.GetGrain<IStatefulOperator>(unit.PrimaryKey, Constants.Stateful_Operator_Prefix);
                    statefulOperator.Recovery(msg);
                }
                else if (unit.OperatorType == OperatorType.Stateless)
                {
                    IStatelessOperator statelessOperator = GrainFactory.GetGrain<IStatelessOperator>(unit.PrimaryKey, Constants.Stateless_Operator_Prefix);
                    statelessOperator.Recovery(msg);
                }
                else
                {
                    throw new ArgumentException("Recovery: The operator type is in valid!");
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

        public Task<List<IStreamSource>> GetRandomSources(int num)
        {
            CheckNum(num);
            List<IStreamSource> sources = new List<IStreamSource>();
            for (int i = 0; i < num; i++)
            {
                var source = GrainFactory.GetGrain<IStreamSource>(Guid.NewGuid());
                sources.Add(source);
            }
            return Task.FromResult(sources);
        }

        public Task<List<IOperator>> GetRandomStatefulOperators(int num)
        {
            CheckNum(num);
            List<IOperator> statefulOps = new List<IOperator>();
            for (int i = 0; i < num; i++)
            {
                var op = GrainFactory.GetGrain<IStatefulOperator>(Guid.NewGuid(), Constants.Stateful_Operator_Prefix);
                statefulOps.Add(op);
            }
            return Task.FromResult(statefulOps);
        }

        public Task<List<IOperator>> GetRandomStatelessOperators(int num)
        {
            CheckNum(num);
            List<IOperator> statelessOps = new List<IOperator>();
            for (int i = 0; i < num; i++)
            {
                var op = GrainFactory.GetGrain<IStatelessOperator>(Guid.NewGuid(), Constants.Stateless_Operator_Prefix);
                statelessOps.Add(op);
            }
            return Task.FromResult(statelessOps);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> AddCustomeOperatorsToSources(List<IStreamSource> sources, List<IOperator> operators)
        {
            List<TopologyUnit> units = new List<TopologyUnit>();
            foreach (var op in operators)
            {
                units.Add(await op.GetTopologyUnit());
            }

            foreach (var source in sources)
            {
                source.AddCustomDownStreamOperators(units);
            }

            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> AddCustomeOperatorsToNonSourceOperators(List<IOperator> upperOps, List<IOperator> downOps)
        {
            List<TopologyUnit> units = new List<TopologyUnit>();
            foreach (var op in downOps)
            {
                units.Add(await op.GetTopologyUnit());
            }

            foreach (var op in upperOps)
            {
                op.AddCustomDownStreamOperators(units);
                PrettyConsole.Line("Add 1");
            }

            return Task.CompletedTask;
        }

        private void CheckNum(int num)
        {
            if (num <= 0)
            {
                throw new ArgumentException("The num must be greater than 0");
            }
        }
    } 
}
