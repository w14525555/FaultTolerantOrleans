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

    public class StatefulStreamOperator : Grain, IStatefulOperator
    {
        private Dictionary<string, int> statesMap = new Dictionary<string, int>();
        private Dictionary<int, Dictionary<string, int>> reverseLogMap = new Dictionary<int, Dictionary<string, int>>();
        private Dictionary<int, Dictionary<string, int>> incrementalLogMap = new Dictionary<int, Dictionary<string, int>>();
        private List<StreamMessage> messageBuffer = new List<StreamMessage>();
        private bool isOperatorFailed = false;
        private const int Default_ZERO = 0;
        private int numberOfUpStream = 0;
        private int numberCurrentBatchBarrierReceived = 0;
        private int numberCurrentBatchCommitReceived = 0;
        private int numberCurrentRecoveryCommitReceived = 0;
        private IBatchTracker batchTracker;
        private IAsyncStream<StreamMessage> asyncStream;
        private ITopology topologyManager;
        private TopologyUnit topologyUnit;

        private int currentBatchID;
        public OperatorSettings operatorSettings = new OperatorSettings();

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
            topologyManager.UpdateOperatorSettings(topologyUnit.primaryKey, operatorSettings);
            return Task.CompletedTask;
        }

        //This function get the words and count
        public async Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            //At frist, if it is a new batch, just creat the incremental log 
            //for it
            if (!incrementalLogMap.ContainsKey(msg.BatchID) && msg.Value != Constants.Recovery_Value)
            {
                var newIncrementalLog = new Dictionary<string, int>();
                var newReverseLog = new Dictionary<string, int>();
                incrementalLogMap.Add(msg.BatchID, newIncrementalLog);
                if (reverseLogMap.ContainsKey(msg.BatchID))
                {
                    PrettyConsole.Line("Error!");
                }
                reverseLogMap.Add(msg.BatchID, newReverseLog);
                PrettyConsole.Line("Add reverse log for batch: " + msg.BatchID);
            }

            if (msg.BatchID > currentBatchID && msg.Value != Constants.Recovery_Value)
            {
                messageBuffer.Add(msg);
                asyncStream = stream;
            }
            else if (msg.BatchID == currentBatchID || msg.Value == Constants.Recovery_Value)
            {
                if (msg.Key != Constants.System_Key)
                {
                    await CountWord(msg, stream);
                }
                else
                {
                    await ProcessSpecialMessage(msg);
                }
            }
            else
            {
                throw new InvalidOperationException(msg.Key + " " + msg.Value + " The id " + msg.BatchID + " is less than the currentID");
            }
            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Await.Warning", "CS4014:Await.Warning")]
        private Task CountWord(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            if (statesMap.ContainsKey(msg.Key))
            {
                //Thrid time throw a exception
                if (msg.Key == "me" && statesMap[msg.Key] == 2)
                {
                    throw new EndOfStreamException();
                }
                UpdateReverseLog(msg);
                statesMap[msg.Key]++;
                UpdateIncrementalLog(msg);
                stream.OnNextAsync(new StreamMessage(msg.Key, statesMap[msg.Key].ToString()));
            }
            else
            {
                statesMap.Add(msg.Key, 1);
                //If insert, only save the key into reverse log
                reverseLogMap[msg.BatchID].Add(msg.Key, Default_ZERO);
                incrementalLogMap[msg.BatchID].Add(msg.Key, 1);

                stream.OnNextAsync(new StreamMessage(msg.Key, "1"));
            }
            return Task.CompletedTask;
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
                    PrettyConsole.Line("Start process normal message");
                    await ProcessNormalMessagesInTheBuffer(currentBatchID + 1);
                    numberCurrentBatchBarrierReceived = 0;
                }
            }
            else if (msg.Value == Constants.Commit_Value)
            {
                PrettyConsole.Line("A stateful grain" + "Clear Reverse log and save Incremental log: " + msg.BatchID);
                   
                numberCurrentBatchCommitReceived++;
                await batchTracker.CompleteOneOperatorCommit(msg.barrierOrCommitInfo);
                //When all the commit message received, increment the batch id 
                if (numberCurrentBatchCommitReceived == numberOfUpStream)
                {
                    await SaveIncrementalLogIntoStorage();
                    numberCurrentBatchCommitReceived = 0;
                    ClearIncrementalLog(msg.BatchID);
                    ClearReverseLog(msg.BatchID);
                    currentBatchID++;
                    PrettyConsole.Line("LILILILILI");
                    if (messageBuffer.Count > 0)
                    {
                        await ProcessSpecialMessagesInTheBuffer();
                    }
                }
            }
            else if (msg.Value == Constants.Recovery_Value)
            {
                if (numberCurrentRecoveryCommitReceived == 0)
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
                }
                numberCurrentRecoveryCommitReceived++;
                if (numberCurrentRecoveryCommitReceived == numberOfUpStream)
                {
                    numberCurrentRecoveryCommitReceived = 0;
                }
                await batchTracker.CompleteOneOperatorRecovery(msg.barrierOrCommitInfo);
            }
            return Task.CompletedTask;
        }

        private Task ClearIncrementalLog(int batchID)
        {
            incrementalLogMap.Remove(batchID);
            return Task.CompletedTask;
        }

        private async Task<Task> RecoveryFromReverseLogOrIncrementalLog(int batchID)
        {
            if (isOperatorFailed)
            {
                try
                {
                    await RevertStateFromIncrementalLog();
                }
                catch (Exception e)
                {
                    PrettyConsole.Line("Exception of read documents : " + e);
                }
            }
            else
            {
                await RevertStateFromReverseLog(batchID + 1);
            }
            return Task.CompletedTask;
        }

        public Task ClearReverseLog(int batchID)
        {
            if (reverseLogMap.ContainsKey(batchID))
            {
                reverseLogMap.Remove(batchID);
            }
            else
            {
                PrettyConsole.Line("Doesn't contain reverse log of batch: " + batchID);
                throw new ArgumentException("Doesn't contain reverse log of batch: " + batchID);
            }
            return Task.CompletedTask;
        }

        public async Task<Task> SaveIncrementalLogIntoStorage()
        {
            //Once save the state to files, then clear
            //The incremental log
            var incrementalLog = await GetIncrementalLog(currentBatchID);
            await SaveStateToFile(incrementalLog);
            //incrementalLogMap.Remove(currentBatchID);
            PrettyConsole.Line("Clear incremental log of batch " + currentBatchID +"  after save in disk");
            return Task.CompletedTask;
        }

        private Task<Dictionary<string, int>>GetIncrementalLog(int batchID)
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

        private Task<Dictionary<string, int>> GetReverseLog(int batchID)
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

        private async Task<Task> UpdateIncrementalLog(StreamMessage msg)
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

        private async Task<Task> UpdateReverseLog(StreamMessage msg)
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

        //This method is used to process the special message after commit
        private async Task<Task> ProcessSpecialMessagesInTheBuffer()
        {
            if (messageBuffer.Count > 0)
            {
                if (asyncStream != null)
                {
                    for (int i = 0; i< messageBuffer.Count; i++)
                    {
                        var msg = messageBuffer[i];
                        if (msg.BatchID == currentBatchID && msg.Key == Constants.System_Key)
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

        //This method is used to process the normal message after barrier
        private async Task<Task> ProcessNormalMessagesInTheBuffer(int batchID)
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
                            await CountWord(msg, asyncStream);
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

        private Task SaveStateToFile(Dictionary<string, int> state)
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

        public async Task<Task> RevertStateFromIncrementalLog()
        {
            PrettyConsole.Line("This grain is restarted! Recovery from Incremental log");
            List<Dictionary<string, int>> logs = await ReadFromBinaryFile<Dictionary<string, int>>(operatorSettings.incrementalLogAddress);
            if (logs.Count == 0)
            {
                statesMap.Clear();
            }
            else if (logs.Count == 1)
            {
                statesMap = logs[0];
            }
            else
            {
                await CalculateStatesFromIncrementalLog(logs);
            }
            return Task.CompletedTask;
        }

        private Task CalculateStatesFromIncrementalLog(List<Dictionary<string, int>> logs)
        {
            foreach (var log in logs)
            {
                foreach (var item in log)
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
            return Task.CompletedTask;
        }

        public Task IncrementNumberOfUpStreamOperator()
        {
            numberOfUpStream++;
            PrettyConsole.Line("The number of upstream is " + numberOfUpStream);
            return Task.CompletedTask;
        }

        public Task<TopologyUnit> GetTopologyUnit()
        {
            return Task.FromResult(topologyUnit);
        }

    }
}
