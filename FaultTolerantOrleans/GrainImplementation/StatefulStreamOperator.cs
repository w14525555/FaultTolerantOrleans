using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using Utils;
using SystemInterfaces;
using SystemInterfaces.Model;
using Orleans.Streams;
using System.IO;
using System;

namespace GrainImplementation
{
    //Once a consumer grain receive messages, it should increment itself
    //when it finishes, it should tell the tracker that the task is completed. 

    public abstract class StatefulStreamOperator : Grain, IStatefulOperator
    {
        private Dictionary<string, int> statesMap = new Dictionary<string, int>();
        private Dictionary<int, Dictionary<string, int>> reverseLogMap = new Dictionary<int, Dictionary<string, int>>();
        private Dictionary<int, Dictionary<string, int>> incrementalLogMap = new Dictionary<int, Dictionary<string, int>>();
        private Dictionary<int, Dictionary<Guid, int>> upStreamMessageCountMaps = new Dictionary<int, Dictionary<Guid, int>>();
        private Dictionary<int, Dictionary<Guid, int>> downStreamMessageCountMaps = new Dictionary<int, Dictionary<Guid, int>>();
        private List<StreamMessage> messageBuffer = new List<StreamMessage>();
        protected OperatorSettings operatorSettings = new OperatorSettings();
        protected TopologyUnit topologyUnit;

        protected bool isOperatorFailed = false;
        protected bool isARestartOperator = false;
        protected const int Default_ZERO = 0;
        protected int numberOfUpStream = 0;
        protected int numberCurrentBatchBarrierReceived = 0;
        protected int currentBatchID;
        private int currentReverseLogID = 0;

        protected IBatchTracker batchTracker;
        protected IAsyncStream<StreamMessage> asyncStream;
        protected ITopology topologyManager;
        protected List<IOperator> downStreamOperators = new List<IOperator>();

        public override Task OnActivateAsync()
        {
            //Add a initial state for testing usage
            currentBatchID = 0;
            //Generate random file name
            var name = @"D:\grainStates\grain" + Guid.NewGuid().ToString() + ".dat";
            batchTracker = GrainFactory.GetGrain<IBatchTracker>(Constants.Tracker);
            operatorSettings.incrementalLogAddress = name;
            operatorSettings.operatorType = OperatorType.Stateful;
            topologyManager = GrainFactory.GetGrain<ITopology>(Constants.Topology_Manager);
            topologyUnit = new TopologyUnit(OperatorType.Stateful, this.GetPrimaryKey());
            topologyManager.AddUnit(topologyUnit);
            topologyManager.UpdateOperatorSettings(topologyUnit.PrimaryKey, operatorSettings);
            return Task.CompletedTask;
        }

        public abstract Task CustomExecutionMethod(StreamMessage msg, IAsyncStream<StreamMessage> stream);

        //This function get the words and count
        public async Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            //At frist, if it is a new batch, just creat the incremental log 
            //for it
            if (!incrementalLogMap.ContainsKey(msg.BatchID))
            {
                var newIncrementalLog = new Dictionary<string, int>();
                var newReverseLog = new Dictionary<string, int>();
                incrementalLogMap.Add(msg.BatchID, newIncrementalLog);
                reverseLogMap.Add(msg.BatchID, newReverseLog);
            }

            if (msg.BatchID > currentReverseLogID)
            {
                currentReverseLogID = msg.BatchID;
            }

            if (msg.BatchID > currentBatchID)
            {
                messageBuffer.Add(msg);
                asyncStream = stream;
            }
            else if (msg.BatchID == currentBatchID || msg.Key == Constants.System_Key)
            {
                if (msg.Key != Constants.System_Key)
                {
                    await IncrementUpStreamCount(msg);
                    await CustomExecutionMethod(msg, stream);
                }
                else
                {
                    if (CheckCount(msg))
                    {
                        await ProcessSpecialMessage(msg);
                    }
                }
            }
            else
            {
                throw new InvalidOperationException(msg.Key + " " + msg.Value + " The id " + msg.BatchID + " is less than the currentID:" + currentBatchID);
            }
            return Task.CompletedTask;
        }

        private Task IncrementUpStreamCount(StreamMessage msg)
        {
            int batchID = msg.BatchID;
            if (!upStreamMessageCountMaps.ContainsKey(batchID))
            {
                upStreamMessageCountMaps.Add(batchID, new Dictionary<Guid, int>());
            }

            if (upStreamMessageCountMaps[batchID].ContainsKey(msg.From))
            {
                upStreamMessageCountMaps[batchID][msg.From] = upStreamMessageCountMaps[batchID][msg.From] + 1;
            }
            else
            {
                upStreamMessageCountMaps[batchID].Add(msg.From, 1);
            }
            return Task.CompletedTask;
        }

        private bool CheckCount(StreamMessage msg)
        {
            if (!upStreamMessageCountMaps.ContainsKey(msg.BatchID))
            {
                if (msg.Count == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else if (upStreamMessageCountMaps[msg.BatchID].ContainsKey(msg.From) && msg.Count == upStreamMessageCountMaps[msg.BatchID][msg.From])
            {
                return true;
            }
            else if (!upStreamMessageCountMaps[msg.BatchID].ContainsKey(msg.From) && msg.Count == 0)
            {
                return true;
            }
            else
            {
                PrettyConsole.Line("The count in stateful operator is not equal!");
                throw new InvalidOperationException("The count in stateful operator is not equal!");
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        private async Task<Task> ProcessSpecialMessage(StreamMessage msg)
        {
            if (msg.Value == Constants.Barrier_Value)
            {
                //Just complete the tracking
                await batchTracker.CompleteOneOperatorBarrier(msg.barrierOrCommitInfo);
                numberCurrentBatchBarrierReceived++;
                if (numberOfUpStream == numberCurrentBatchBarrierReceived)
                {
                    //PrettyConsole.Line("Start process normal message");
                    await ProcessMessagesInTheBuffer(currentBatchID + 1);
                    numberCurrentBatchBarrierReceived = 0;
                }
            }
            return Task.CompletedTask;
        }

        //Commit Logic
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> Commit(StreamMessage msg)
        {
            //1.Save log into storage
            SaveIncrementalLogIntoStorage();
            ClearIncrementalLog(msg.BatchID);
            ClearReverseLog(msg.BatchID);
            currentBatchID++;
            ClearCountMap(msg.BatchID);
            await batchTracker.CompleteOneOperatorCommit(msg.barrierOrCommitInfo);
            return Task.CompletedTask;
        }

        private void ClearCountMap(int batchID)
        {
            if (upStreamMessageCountMaps.ContainsKey(batchID))
            {
                upStreamMessageCountMaps.Remove(batchID);
            }
            if (downStreamMessageCountMaps.ContainsKey(batchID))
            {
                downStreamMessageCountMaps.Remove(batchID);
            }
        }

        public async Task<Task> Recovery(StreamMessage msg)
        {
            //If negative 1, means there is no committed bathc
            if (msg.BatchID == -1)
            {
                statesMap.Clear();
                reverseLogMap.Clear();
                incrementalLogMap.Clear();
                currentBatchID = 0;
            }
            else
            {
                //1. Recovery From the reverse log or incremental log
                await RecoveryFromReverseLogOrIncrementalLog(msg.BatchID);
                //2. Clear the buffer
                messageBuffer.Clear();
                //3. Clear the reverse log and incremental log
                //4. Reset batch ID, the current ID should greatea than the committed id 
                currentBatchID = msg.BatchID + 1;
            }
            await batchTracker.CompleteOneOperatorRecovery(msg.barrierOrCommitInfo);
            return Task.CompletedTask;
        }

        private Task ClearIncrementalLog(int batchID)
        {
            if (incrementalLogMap.ContainsKey(batchID))
            {
                incrementalLogMap.Remove(batchID);
            }
            return Task.CompletedTask;
        }

        private async Task<Task> RecoveryFromReverseLogOrIncrementalLog(int batchID)
        {
            if (isOperatorFailed)
            {
                try
                {
                    await RevertStateFromIncrementalLog(batchID);
                }
                catch (Exception e)
                {
                    PrettyConsole.Line("Exception of read documents : " + e);
                }
            }
            else
            {
                for (int i = currentReverseLogID; i >= batchID + 1; i--)
                {
                    await RevertStateFromReverseLog(i);
                }
            }
            return Task.CompletedTask;
        }

        public Task ClearReverseLog(int batchID)
        {
            if (reverseLogMap.ContainsKey(batchID))
            {
                reverseLogMap.Remove(batchID);
            }
            return Task.CompletedTask;
        }

        public async Task<Task> SaveIncrementalLogIntoStorage()
        {
            //Once save the state to files, then clear
            //The incremental log
            var incrementalLog = await GetIncrementalLog(currentBatchID);
            await SaveStateToFile(new IncrementalLog(incrementalLog, currentBatchID));
            //incrementalLogMap.Remove(currentBatchID);
            return Task.CompletedTask;
        }

        protected Task<Dictionary<string, int>>GetIncrementalLog(int batchID)
        {
            if (incrementalLogMap.ContainsKey(batchID))
            {
                return Task.FromResult(incrementalLogMap[batchID]);
            }
            else
            {
                throw new InvalidOperationException("The incremental log of batch " + batchID + " is not exist");
            }
        }

        protected Task<Dictionary<string, int>> GetReverseLog(int batchID)
        {
            if (reverseLogMap.ContainsKey(batchID))
            {
                return Task.FromResult(reverseLogMap[batchID]);
            }
            else
            {
                throw new InvalidOperationException("The reverse log of batch " + batchID + " is not exist");
            }
        }

        protected async Task<Task> UpdateIncrementalLog(StreamMessage msg)
        {
            var incrementalLog = await GetIncrementalLog(msg.BatchID);
            if (incrementalLog.ContainsKey(msg.Key))
            {
                incrementalLog[msg.Key] = statesMap[msg.Key];
            }
            else
            {
                incrementalLog.Add(msg.Key, statesMap[msg.Key]);
            }
            return Task.CompletedTask;
        }

        protected async Task<Task> UpdateReverseLog(StreamMessage msg)
        {
            var reverseLog = await GetReverseLog(msg.BatchID);
            //If reverse log contains the key, means 
            //it has the value of last batch
            if (!reverseLog.ContainsKey(msg.Key))
            {
                reverseLog.Add(msg.Key, statesMap[msg.Key]);
            }
            return Task.CompletedTask;
        }

        //This method is used to process the normal message after barrier
        protected async Task<Task> ProcessMessagesInTheBuffer(int batchID)
        {
            if (messageBuffer.Count > 0)
            {
                if (asyncStream != null)
                {
                    for (int i = 0; i < messageBuffer.Count; i++)
                    {
                        var msg = messageBuffer[i];
                        if (msg.BatchID == batchID && msg.Key != Constants.System_Key)
                        {
                            await CustomExecutionMethod(msg, asyncStream);
                        }
                        else if (msg.BatchID == batchID && msg.Key == Constants.System_Key)
                        {
                            await ExecuteMessage(messageBuffer[i], asyncStream);
                        }
                    }
                }
                else
                {
                    throw new InvalidOperationException("Process Buffer Message: no Stream!");
                }
            }
            return Task.CompletedTask;
        }

        protected Task SaveStateToFile(IncrementalLog state)
        {
            PrettyConsole.Line("Save the incremental log to " + operatorSettings.incrementalLogAddress);
            try
            {
                WriteToBinaryFile(operatorSettings.incrementalLogAddress, state);
            }
            catch (Exception e)
            {
                PrettyConsole.Line("Error " + e);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Writes the given object instance to a binary file.
        public static Task WriteToBinaryFile<T>(string filePath, T objectToWrite, bool append = true)
        {
            using (Stream stream = File.Open(filePath, append ? FileMode.Append : FileMode.Create))
            {
                var binaryFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                binaryFormatter.Serialize(stream, objectToWrite);
            }

            return Task.CompletedTask;
        }

        public static Task<List<T>> ReadFromBinaryFile<T>(string filePath)
        {
            using (Stream stream = File.Open(filePath, FileMode.OpenOrCreate))
            {
                List<T> statesList = new List<T>();
                while (stream.Position < stream.Length)
                {
                    var binaryFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                    T obj = (T)binaryFormatter.Deserialize(stream);
                    statesList.Add(obj);
                }
                return Task.FromResult(statesList);
            }
        }

        public async Task<Task> RevertStateFromReverseLog(int batchID)
        {
            //Here three cases rollback the states
            //in reverse log. 
            //Insert: Should remove the value from map
            //Update: revert the value
            //delete: add the key and value back
            PrettyConsole.Line("Revert from Reverse Log!");
            //If the reverse log does not contain the batch id
            //means there is no change in the states map
            if (reverseLogMap.ContainsKey(batchID))
            {
                var reverseLog = await GetReverseLog(batchID);
                foreach (var item in reverseLog)
                {
                    //If delete, the statemap does not contain the key
                    if (!statesMap.ContainsKey(item.Key))
                    {
                        statesMap.Add(item.Key, item.Value);
                    }
                    else
                    {
                        //If null, means it was inserted value
                        if (item.Value == Default_ZERO)
                        {
                            statesMap.Remove(item.Key);
                        }
                        else
                        {
                            //The last case is reverting updated value 
                            statesMap[item.Key] = item.Value;
                        }
                    }
                }
            }
            
            return Task.CompletedTask;
        }

        public async Task<Task> RevertStateFromIncrementalLog(int batchID)
        {
            PrettyConsole.Line("This grain is restarted! Recovery from Incremental log");
            List<IncrementalLog> logs = await ReadFromBinaryFile<IncrementalLog>(operatorSettings.incrementalLogAddress);
            if (logs.Count == 0)
            {
                statesMap.Clear();
            }
            else if (logs.Count == 1)
            {
                statesMap = logs[0].Log;
            }
            else
            {
                await CalculateStatesFromIncrementalLog(logs);
            }
            return Task.CompletedTask;
        }

        private Task CalculateStatesFromIncrementalLog(List<IncrementalLog> logs)
        {
            for (int i = logs.Count -1; i>=0; i++)
            {
                foreach (var item in logs[i].Log)
                {
                    if (statesMap.ContainsKey(item.Key))
                    {
                        statesMap[item.Key] = item.Value;
                    }
                    else
                    {
                        statesMap.Add(item.Key, item.Value);
                    }
                }
            }
            return Task.CompletedTask;
        }

        public Task<OperatorSettings> GetOperatorSettings()
        {
            return Task.FromResult(operatorSettings);
        }

        public Task LoadSettings(OperatorSettings operatorSettings)
        {
            this.operatorSettings = operatorSettings;
            return Task.CompletedTask;
        }

        public Task<int> GetState(string key)
        {
            if (statesMap.ContainsKey(key))
            {
                return Task.FromResult(statesMap[key]);
            }
            else
            {
                return Task.FromResult(-1);
            }
        }

        public async Task<int> GetStateInReverseLog(string key)
        {
            if (reverseLogMap.ContainsKey(currentBatchID))
            {
                var reverseLog = await GetReverseLog(currentBatchID);
                if (reverseLog.ContainsKey(key))
                {
                    return await Task.FromResult(reverseLog[key]);
                }
            }

            return await Task.FromResult(-1);
        }

        public async Task<int> GetStateInIncrementalLog(string key)
        {
            if (incrementalLogMap.ContainsKey(currentBatchID))
            {
                var incrementalLog = await GetIncrementalLog(currentBatchID);
                if (incrementalLog.ContainsKey(key))
                {
                    return await Task.FromResult(incrementalLog[key]);
                }
            }
            return await Task.FromResult(-1);

        }

        public Task MarkOperatorAsFailed()
        {
            isOperatorFailed = true;
            isARestartOperator = true;
            PrettyConsole.Line("Mark this as failed!");
            return Task.CompletedTask;
        }

        public Task IncrementNumberOfUpStreamOperator()
        {
            numberOfUpStream++;
            PrettyConsole.Line("Increase: The number of upstream is " + numberOfUpStream);
            return Task.CompletedTask;
        }

        public Task DecreseNumberOfUpStreamOperator()
        {
            numberOfUpStream--;
            PrettyConsole.Line("Decrease: The number of upstream is " + numberOfUpStream);
            return Task.CompletedTask;
        }

        public Task<TopologyUnit> GetTopologyUnit()
        {
            return Task.FromResult(topologyUnit);
        }

        protected bool CheckStatesMapConstainTheKey(string key)
        {
            return statesMap.ContainsKey(key);
        }

        protected int GetValueFromStatesMap(string key)
        {
            if (CheckStatesMapConstainTheKey(key))
            {
                return statesMap[key];
            }
            else
            {
                throw new ArgumentException("Try to get un-exist value from states map");
            }
        }

        protected async Task<Task> UpdateStatesMap(StreamMessage msg, int newValue)
        {
            if (CheckStatesMapConstainTheKey(msg.Key))
            {
                await UpdateReverseLog(msg);
                statesMap[msg.Key] = newValue;
                await UpdateIncrementalLog(msg);
                return Task.CompletedTask;
            }
            else
            {
                throw new ArgumentException("Try to Update un-exist value from states map");
            }
        }

        protected Task InsertIntoStatesMap(StreamMessage msg, int initialValue)
        {
            if (!CheckStatesMapConstainTheKey(msg.Key))
            {
                statesMap.Add(msg.Key, initialValue);
                //If insert, only save the key into reverse log
                reverseLogMap[msg.BatchID].Add(msg.Key, Default_ZERO);
                incrementalLogMap[msg.BatchID].Add(msg.Key, initialValue);
                return Task.CompletedTask;
            }
            else
            {
                throw new ArgumentException("Try to Inset already exist value to states map");
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        public async Task<Task> AddCustomDownStreamOperators(List<TopologyUnit> units)
        {
            foreach (var unit in units)
            {
                if (unit.OperatorType == OperatorType.Stateful)
                {
                    var op = GrainFactory.GetGrain<IStatefulOperator>(unit.PrimaryKey, Constants.Stateful_Operator_Prefix);
                    op.IncrementNumberOfUpStreamOperator();
                    downStreamOperators.Add(op);
                    operatorSettings.AddOpratorToDict(op.GetPrimaryKey(), await op.GetOperatorSettings());
                    topologyManager.ConnectUnits(topologyUnit.PrimaryKey, op.GetPrimaryKey());
                }
                else if (unit.OperatorType == OperatorType.Stateless)
                {
                    var op = GrainFactory.GetGrain<IStatelessOperator>(unit.PrimaryKey, Constants.Stateless_Operator_Prefix);
                    downStreamOperators.Add(op);
                    topologyManager.ConnectUnits(topologyUnit.PrimaryKey, op.GetPrimaryKey());
                }
                else
                {
                    throw new ArgumentException("The down stream operor cannot be a source");
                }
            }
            topologyManager.UpdateOperatorSettings(topologyUnit.PrimaryKey, operatorSettings);
            return Task.CompletedTask;
        }

        public Task RemoveCustomDownStreamOperator(Guid guid)
        {
            int index = -1;
            for (int i = 0; i < downStreamOperators.Count; i++)
            {
                if (downStreamOperators[i].GetPrimaryKey() == guid)
                {
                    index = i;
                    break;
                }
            }
            if (index != -1)
            {
                downStreamOperators.RemoveAt(index);
                PrettyConsole.Line("Remove old stateful from upper stream");
                operatorSettings.RemoveOperatorFromDict(guid);
                topologyManager.UpdateOperatorSettings(this.GetPrimaryKey(), operatorSettings);
            }
            else
            {
                throw new ArgumentException();
            }

            return Task.CompletedTask;
        }
    }
}
