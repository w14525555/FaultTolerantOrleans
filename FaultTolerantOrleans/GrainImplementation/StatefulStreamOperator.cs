using Orleans;
using GrainInterfaces;
using System.Threading.Tasks;
using GrainInterfaces.Model;
using System.Collections.Generic;
using System;
using Utils;
using System.IO;

namespace GrainImplementation
{
    //Once a consumer grain receive messages, it should increment itself
    //when it finishes, it should tell the tracker that the task is completed. 

    public class StatefulStreamOperator : Grain, IOperator
    {
        private Dictionary<string, string> statesMap = new Dictionary<string, string>();
        private Dictionary<string, string> reverseLog = new Dictionary<string, string>();
        private Dictionary<string, string> incrementalLog = new Dictionary<string, string>();
        
        public override Task OnActivateAsync()
        {
            //Add a initial state for testing usage
            statesMap.Add("initial", "hello");
            reverseLog.Add("initial", null);
            incrementalLog.Add("initial", "hello");
            return Task.CompletedTask;
        }

        public Task ConsumeMessage(StreamMessage msg)
        {
            if (msg.operation != Operation.Null)
            {
                if (msg.operation == Operation.Delete)
                {
                    DeleteOperation(msg);
                }
                else if (msg.operation == Operation.Insert)
                {
                    InsertOperation(msg);
                }
                else if(msg.operation == Operation.Update)
                {
                    UpdateOperation(msg);
                }
            }
            return Task.CompletedTask;
        }

        public Task<string> GetState(string key)
        {
            if (statesMap.ContainsKey(key))
            {
                return Task.FromResult(statesMap[key]);
            }
            else
            {
                return Task.FromResult("Not Exist");
            }
        }

        public Task<string> GetStateInReverseLog(string key)
        {
            if (reverseLog.ContainsKey(key))
            {
                return Task.FromResult(reverseLog[key]);
            }
            else
            {
                return Task.FromResult("Not Exist");
            }
        }

        public Task<string> GetStateInIncrementalLog(string key)
        {
            if (incrementalLog.ContainsKey(key))
            {
                return Task.FromResult(incrementalLog[key]);
            }
            else
            {
                return Task.FromResult("Not Exist");
            }
        }

        private Task DeleteOperation(StreamMessage msg)
        {
            if (statesMap.ContainsKey(msg.Key))
            {
                HandleReverseLogOnDelete(msg.Key);
                statesMap.Remove(msg.Key);
                HandleIncrementalLogOnDelete(msg);
            }
            return Task.CompletedTask;
        }


        private Task InsertOperation(StreamMessage msg)
        {
            if (!statesMap.ContainsKey(msg.Key))
            {
                HandleReverseLogOnInsert(msg.Key);
                statesMap.Add(msg.Key, msg.Value);
                HandleIncrementalLogOnInsert(msg);
            }
            return Task.CompletedTask;
        }

        public Task UpdateOperation(StreamMessage msg)
        {
            if (statesMap.ContainsKey(msg.Key))
            {
                HandleReverseLogOnUpdate(msg.Key);
                statesMap[msg.Key] = msg.Value;
                HandleIncrementalLogOnUpdate(msg);
            }
            return Task.CompletedTask;
        }

        private Task HandleReverseLogOnDelete(string key)
        {
            if (!reverseLog.ContainsKey(key))
            {
                reverseLog.Add(key, statesMap[key]);
            }
            else
            {
                //save the value before deleting it
                reverseLog[key] = statesMap[key];
            }
            return Task.CompletedTask;
        }

        private Task HandleReverseLogOnInsert(string key)
        {
            if (reverseLog.ContainsKey(key))
            {
                reverseLog.Add(key, null);
            }
            else
            {
                //only add the key
                reverseLog.Add(key, null);
            }
            return Task.CompletedTask;
        }

        private Task HandleReverseLogOnUpdate(string key)
        {
            if (!reverseLog.ContainsKey(key))
            {
                reverseLog.Add(key, statesMap[key]);
            }
            else
            {
                //Save the value before changing it
                reverseLog[key] = statesMap[key];
            }
            return Task.CompletedTask;
        }

        private Task HandleIncrementalLogOnInsert(StreamMessage msg)
        {
            if (incrementalLog.ContainsKey(msg.Key))
            {
                incrementalLog[msg.Key] = msg.Value;
            }
            else
            {
                //Add key and value 
                incrementalLog.Add(msg.Key, msg.Value);
            }
            return Task.CompletedTask;
        }

        private Task HandleIncrementalLogOnUpdate(StreamMessage msg)
        {
            if (!incrementalLog.ContainsKey(msg.Key))
            {
                incrementalLog.Add(msg.Key, msg.Value);
            }
            else
            {
                //Save the value before changing it
                incrementalLog[msg.Key] = msg.Value;
            }
            return Task.CompletedTask;
        }

        private Task HandleIncrementalLogOnDelete(StreamMessage msg)
        {
            if (!incrementalLog.ContainsKey(msg.Key))
            {
                incrementalLog.Add(msg.Key, msg.Value);
            }
            else
            {
                //save the deleted key
                incrementalLog[msg.Key] = null;
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

        private Task SaveStateToFile(Dictionary<string, string> state)
        {
            PrettyConsole.Line("Save the incremental log to D:\batch.dat");
            try
            {
                WriteToBinaryFile(@"D:\batch.dat", state);
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

        //public static Task<T> ReadFromBinaryFile<T>(string filePath)
        //{
        //    using (Stream stream = File.Open(filePath, FileMode.Open))
        //    {
        //        var binaryFormatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
        //        return Task.FromResult((T)binaryFormatter.Deserialize(stream));
        //    }
        //}

    }
}
