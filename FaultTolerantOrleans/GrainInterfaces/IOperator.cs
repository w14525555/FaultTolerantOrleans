﻿using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IOperator: IGrainWithGuidKey
    {
        Task<Task> AddCustomDownStreamOperators(List<TopologyUnit> units);
        Task RemoveCustomDownStreamOperator(Guid guid);
        Task<TopologyUnit> GetTopologyUnit();
        Task<Task> ExecuteMessage(StreamMessage msg, IAsyncStream<StreamMessage> stream);
        Task<int> DetectErrors();
        //Methods for testing
        Task<int> GetStateInReverseLog(string word);
        Task<int> GetStateInIncrementalLog(string word);
        Task<int> GetState(string word);

    }
}
