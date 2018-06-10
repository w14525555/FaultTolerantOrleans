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
        private IBatchTracker batchTracker;
        public OperatorSettings operatorSettings;

        public override Task OnActivateAsync()
        {
            //Add a initial state for testing usage
            operatorSettings = new OperatorSettings();
            Random random = new Random();
            var name = @"D:\grain"+ Guid.NewGuid().ToString() + ".dat";
            operatorSettings.incrementalLogAddress = name;
            return Task.CompletedTask;
        }

        //This function get the words and count
        public Task ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            if (msg.Key != Constants.System_Key)
            {
               CountWord(msg, stream);
            }
            else
            {
                ProcessSpecialMessage(msg);
            }

            return Task.CompletedTask;
        }

        private Task CountWord(StreamMessage msg, IAsyncStream<StreamMessage> stream)
        {
            if (statesMap.ContainsKey(msg.Key))
            {
                //It must be the frist operation in the batch
                if (!reverseLog.ContainsKey(msg.Key))
                {
                    reverseLog.Add(msg.Key, statesMap[msg.Key]);
                }
                statesMap[msg.Key]++;

                stream.OnNextAsync(new StreamMessage(msg.Key, statesMap[msg.Key].ToString()));
            }
            else
            {
                statesMap.Add(msg.Key, 1);
                //If insert, only save the key into reverse log
                reverseLog.Add(msg.Key, 0);
                stream.OnNextAsync(new StreamMessage(msg.Key, "1"));
            }
            return Task.CompletedTask;
        }

        private Task ProcessSpecialMessage(StreamMessage msg)
        {
            if (msg.Value == Constants.Barrier_Value)
            {
                //Just complete the tracking
                msg.barrierInfo.BatchID = msg.BatchID;
                batchTracker.CompleteTracking(msg.barrierInfo);
            }
            else if (msg.Value == Constants.Commit_Value)
            {
                //Commit Here 
                PrettyConsole.Line(IdentityString + " Send comit message for BatchID: " + msg.BatchID);
                ClearReverseLog();
                UpdateIncrementalLog();
            }
            return Task.CompletedTask;
        }

        public Task ClearReverseLog()
        {
            reverseLog.Clear();
            return Task.CompletedTask;
        }

        public Task UpdateIncrementalLog()
        {
            //Once save the state to files, then clear
            //The incremental log
            SaveStateToFile(incrementalLog);
            incrementalLog.Clear();
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
        public static Task WriteToBinaryFile<T>(string filePath, T objectToWrite, bool append = false)
        {
            using (Stream stream = File.Open(filePath, append ? FileMode.Append : FileMode.Create))
            {
                var binaryFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                binaryFormatter.Serialize(stream, objectToWrite);
            }

            return Task.CompletedTask;
        }

        //public Task RevertStateFromReverseLog()
        //{
        //    //Here three cases rollback the states
        //    //in reverse log. 
        //    //Insert: Should remove the value from map
        //    //Update: revert the value
        //    //delete: add the key and value back
        //    foreach (var item in reverseLog)
        //    {
        //        //If delete, the statemap does not contain the key
        //        if (!statesMap.ContainsKey(item.Key))
        //        {
        //            statesMap.Add(item.Key, item.Value);
        //        }
        //        else
        //        {
        //            //If null, means it was inserted value
        //            if (item.Value == null)
        //            {
        //                statesMap.Remove(item.Key);
        //            }
        //            else
        //            {
        //                //The last case is reverting updated value 
        //                statesMap[item.Key] = item.Value;
        //            }
        //        }
        //    }
        //    return Task.CompletedTask;
        //}

        public Task ReloadStateFromIncrementalLog()
        {
            //TODO
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

        public Task SetTracker(IBatchTracker tracker)
        {
            batchTracker = tracker;
            return Task.CompletedTask;
        }

        //public static Task<T> ReadFromBinaryFile<T>(string filePath)
        //{
        //    using (Stream stream = File.Open(filePath, FileMode.Open))
        //    {
        //        var binaryFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
        //        return Task.FromResult((T)binaryFormatter.Deserialize(stream));
        //    }
        //}

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
