using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using SystemInterfaces;
using SystemInterfaces.Model;
using Utils;

namespace GrainImplementation
{
    public class BatchCoodinator : Grain, IBatchCoordinator
    {
        //A Batch Manager should send batch barrier 
        private StreamMessage barrierMsg = new StreamMessage(Constants.System_Key, Constants.Barrier_Value);
        private StreamMessage commitMsg = new StreamMessage(Constants.System_Key, Constants.Commit_Value);
        private StreamMessage recoveryMsg = new StreamMessage(Constants.System_Key, Constants.Recovery_Value);

        private const int Barrier_Interval = 1000;
        private const int Processing_Time_Interval = 100;
        private IDisposable disposable;
        private TimeSpan barrierTimeInterval = TimeSpan.FromMilliseconds(Barrier_Interval);
        private List<int> processingTimeList = new List<int>();

        private List<IStreamSource> sources = new List<IStreamSource>();
        private IBatchTracker tracker;
        private ITopology topologyManager;
        private long numOfWordsProcessed = 0;
        private int startTime;
       

        private int currentBatchID { get; set; }
        private int committedID { get; set; }

        public override Task OnActivateAsync()
        {
            currentBatchID = 0;
            committedID = -1;
            tracker = GrainFactory.GetGrain<IBatchTracker>(Utils.Constants.Tracker);
            topologyManager = GrainFactory.GetGrain<ITopology>(Constants.Topology_Manager);
            PrettyConsole.Line("Register Timer");
            var streamProvider = GetStreamProvider(Constants.FaultTolerantStreamProvider);
            return base.OnActivateAsync();
        }

        public Task AddSourceAndRegisterTimer(IAsyncStream<StreamMessage> stream, IStreamSource source)
        {
            sources.Add(source);
            return Task.CompletedTask;
        }

        public Task StartBarrierTimer()
        {
            disposable = RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
            return Task.CompletedTask;
        }

        private async Task<Task> SendBarrierOnPeriodOfTime(object arg)
        {
            await SendBarrier();
            return Task.CompletedTask;
        }

        public async Task<Task> SendBarrier()
        {
            barrierMsg.BatchID = currentBatchID;
            barrierMsg.barrierOrCommitInfo = new BarrierOrCommitMsgTrackingInfo(Guid.NewGuid(), sources.Count);
            await tracker.TrackingBarrierMessages(barrierMsg);
            foreach (IStreamSource source in sources)
            {
                source.ProduceMessageAsync(barrierMsg);
            }
            currentBatchID++;
            return Task.CompletedTask;
        }

        //Commit 
        public Task StartCommit(int ID)
        {
            PrettyConsole.Line("Start Commit Batch " + ID); 
            commitMsg.BatchID = ID;
            topologyManager.Commit(commitMsg);
            return Task.CompletedTask;
        }

        //Recovery
        public async Task<Task> StartRecovery()
        {
            startTime = System.DateTime.Now.Millisecond;
            //1. Stop the timer
            disposable.Dispose();

            foreach (var source in sources)
            {
                await source.StopSendingMessagesOnRecovery();
            }

            //2. Tell TopologyManager the rollback and reset batchID
            recoveryMsg.BatchID = committedID;
            topologyManager.Recovery(recoveryMsg);
            //3. Clean information in the tracker()
            await tracker.CleanUpOnRecovery();
            //6. Register new timer
            return Task.CompletedTask;
        }

        //Once the recovery completed, just restart the timer
        //Restart the timer
        public async Task<Task> CompleteRecovery(int batchID)
        {
            if (committedID == batchID)
            {
                currentBatchID = batchID + 1;
                disposable = RegisterTimer(SendBarrierOnPeriodOfTime, null, barrierTimeInterval, barrierTimeInterval);
                var detector = GrainFactory.GetGrain<IErrorDetector>(Constants.Error_Detector);
                detector.RegisterTimerToDetectFailures();
                var recoveryTime = System.DateTime.Now.Millisecond - startTime;
                PrettyConsole.Line("Recovery Time: " + recoveryTime);
                return Task.CompletedTask;
            }
            else
            {
                throw new InvalidOperationException("The recvoery batch is not equal to the latest committed ID");
            }
        }

        public async Task<Task> ReplayTheMessagesOnRecoveryCompleted()
        {
            foreach(var source in sources)
            {
                await source.ReplayTheMessageOnRecoveryCompleted();
            }
            return Task.CompletedTask;
        }

        public Task SetCurrentBatchID(int id)
        {
            currentBatchID = id;
            return Task.CompletedTask;
        }

        public Task CompleteCommit(int batchID)
        {
            if (batchID - committedID == 1)
            {
                committedID++;
                PrettyConsole.Line("Committed Batch ID now is: " + committedID);
                return Task.CompletedTask;
            }
            else
            {
                throw new InvalidOperationException("Cannot commit batch greater than committed id 2");
            }
        }

        public Task<int> GetCommittedBatchID()
        {
            return Task.FromResult(committedID);
        }

        public Task AddProcessingTime(int time)
        {
            processingTimeList.Add(time);
            if (processingTimeList.Count >= Processing_Time_Interval)
            {
                numOfWordsProcessed += Processing_Time_Interval;
                if (numOfWordsProcessed % 1000 == 0)
                {
                    PrettyConsole.Line("Process " + numOfWordsProcessed + " words");
                }
                SaveProcessingTimeIntoFiles();
                processingTimeList.Clear();
            }
            return Task.CompletedTask;
        }

        private Task SaveProcessingTimeIntoFiles()
        {
            TextWriter tw = new StreamWriter(@"D:\time.txt", true);
            float time = CalculateAverageProcessingTime();
            tw.WriteLine(time);
            tw.Close();
            return Task.CompletedTask;
        }

        private float CalculateAverageProcessingTime()
        {
            int sum = 0;
            int count = 0;
            foreach (int item in processingTimeList)
            {
                //In the multiprocessor computer, you can get different timing results on different processors
                //So there might be very few negative result. 
                if(item > 0)
                {
                    sum += item;
                    count++;
                }
            }

            if (count == 0)
            {
                throw new InvalidOperationException("All Negative processing time!");
            }
            return (float)sum/(float)count;
        }
    }
}
