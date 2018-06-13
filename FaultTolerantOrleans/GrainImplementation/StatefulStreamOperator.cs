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
        private Dictionary<string, int> reverseLog = new Dictionary<string, int>();
        private Dictionary<string, int> incrementalLog = new Dictionary<string, int>();
        private List<StreamMessage> messageBuffer = new List<StreamMessage>();
        private bool isOperatorFailed = false;
        private const int Default_ZERO = 0;
        private IBatchTracker batchTracker;
        private IAsyncStream<StreamMessage> asyncStream;

        private int currentBatchID;
        public OperatorSettings operatorSettings = new OperatorSettings();

        public override Task OnActivateAsync()
        {
            //Add a initial state for testing usage
            currentBatchID = 0;
            //Generate random file name
            var name = @"D:\grainStates\grain" + Guid.NewGuid().ToString() + ".dat";
            operatorSettings.incrementalLogAddress = name;
            batchTracker = GrainFactory.GetGrain<IBatchTracker>(Constants.Tracker);
            return Task.CompletedTask;
        }

        //This function get the words and count
        public async Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            if (msg.BatchID > currentBatchID)
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

        private Task CountWord(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            if (statesMap.ContainsKey(msg.Key))
            {
                //It must be the frist operation in the batch
                UpdateReverseLog(msg);
                statesMap[msg.Key]++;
                UpdateIncrementalLog(msg);

                stream.OnNextAsync(new StreamMessage(msg.Key, statesMap[msg.Key].ToString()));
            }
            else
            {
                statesMap.Add(msg.Key, 1);
                //If insert, only save the key into reverse log
                reverseLog.Add(msg.Key, Default_ZERO);
                incrementalLog.Add(msg.Key, 1);
                stream.OnNextAsync(new StreamMessage(msg.Key, "1"));
            }
            return Task.CompletedTask;
        }

        private async Task<Task> ProcessSpecialMessage(StreamMessage msg)
        {
            if (msg.Value == Constants.Barrier_Value)
            {
                //Just complete the tracking
                await batchTracker.CompleteOneOperatorBarrier(msg.barrierOrCommitInfo);
            }
            else if (msg.Value == Constants.Commit_Value)
            {
                PrettyConsole.Line("A stateful grain" + "Clear Reverse log and save Incremental log: " + msg.BatchID);
                await ClearReverseLog();
                await SaveIncrementalLogIntoStorage();
                currentBatchID++;
                await ProcessMessagesInTheBuffer();
                await batchTracker.CompleteOneOperatorCommit(msg.barrierOrCommitInfo);
            }
            else if (msg.Value == Constants.Recovery_Value)
            {
                PrettyConsole.Line("Stateful");
                //1. Recovery From the reverse log
                await RecoveryFromReverseLogOrIncrementalLog();
                //2. Clear the buffer
                messageBuffer.Clear();
                //3. Clear the reverse log and incremental log
                reverseLog.Clear();
                incrementalLog.Clear();
                //4. Reset batch ID
                currentBatchID = msg.BatchID;
                await batchTracker.CompleteOneOperatorRecovery(msg.barrierOrCommitInfo);
            }
            return Task.CompletedTask;
        }

        private async Task<Task> RecoveryFromReverseLogOrIncrementalLog()
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
                await RevertStateFromReverseLog();
            }
            return Task.CompletedTask;
        }

        public Task ClearReverseLog()
        {
            reverseLog.Clear();
            return Task.CompletedTask;
        }

        public Task SaveIncrementalLogIntoStorage()
        {
            //Once save the state to files, then clear
            //The incremental log
            SaveStateToFile(incrementalLog);
            incrementalLog.Clear();
            return Task.CompletedTask;
        }

        private Task UpdateIncrementalLog(StreamMessage msg)
        {
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

        private Task UpdateReverseLog(StreamMessage msg)
        {
            if (!reverseLog.ContainsKey(msg.Key))
            {
                reverseLog.Add(msg.Key, statesMap[msg.Key]);
            }
            return Task.CompletedTask;
        }

        private async Task<Task> ProcessMessagesInTheBuffer()
        {
            if (messageBuffer.Count > 0)
            {
                if (asyncStream != null)
                {
                    foreach (var msg in messageBuffer)
                    {
                        if (msg.BatchID == currentBatchID)
                        {
                            await ExecuteMessage(msg, asyncStream);
                        }
                    }
                }
                else
                {
                    throw new InvalidOperationException("Process Buffer Message: now Stream!");
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

        public Task RevertStateFromReverseLog()
        {
            //Here three cases rollback the states
            //in reverse log. 
            //Insert: Should remove the value from map
            //Update: revert the value
            //delete: add the key and value back
            PrettyConsole.Line("Revert from Reverse Log!");
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
            return Task.CompletedTask;
        }

        public async Task<Task> RevertStateFromIncrementalLog()
        {
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

        public Task<OperatorSettings> GetSettings()
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

        public Task<int> GetStateInReverseLog(string key)
        {
            if (reverseLog.ContainsKey(key))
            {
                return Task.FromResult(reverseLog[key]);
            }
            else
            {
                return Task.FromResult(-1);
            }
        }

        public Task<int> GetStateInIncrementalLog(string key)
        {
            if (incrementalLog.ContainsKey(key))
            {
                return Task.FromResult(incrementalLog[key]);
            }
            else
            {
                return Task.FromResult(-1);
            }
        }

    }
}
