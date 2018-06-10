﻿using Orleans;
using Orleans.Streams;
using System.Threading.Tasks;
using SystemInterfaces.Model;

namespace SystemInterfaces
{
    public interface IBatchCoordinator: IGrainWithStringKey
    {
        Task SetChannelAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource channel);

        Task StartCommit(int ID);

        Task StartRecovery();

        Task<Task> SendBarrier();
    }
}
