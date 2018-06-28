#Orleans Fault Tolerant Project

## Definitions:

**BatchCoordinator**: Be resiponsible for sending batch barrier and commit messages to the 
all topology units and handling the recovery process. 

**BatchTracker**: Handling the batch processing and once all the subtasks in one batch have
been finished, tells Batch Coordinator to send commit messages.

**StatefulStreamOperator**: The type of operator that has states. Because of this, it has 
states management mechanism including reverse log and incremental log. 

**StatelessStreamOperator**: Operators that do not have states, so they don't neeed state
management. 

**StreamSouce**: A stream source is a unit that can broadcast messages to its observers.

**Observers**: An Orleans unit that can subscribe to stream source and receive message 
from stream source. 

**StreamBatch**: Constains all tracking information in one batch. 

**BarrierMsgTrackingInfo**: Contains the number and id of tracking information in 
one source.

**StreamMessage**: The stream message. 

**Silo**: can be regarded as server that contains lots of grains. 

**Grains**: The actor unit in Orleans System. 

##System Implementation Details. 

**Batch Processing**: 
Each operator has a buffer to store messages that are not in currentbach. 
Once current batch has been committed, it will start to proceed the messages
of next batches. 

If one operator receive the a message from its source, it firstly checks
which batch it belongs to. If it is not current batch, it will save it into a
message buffer. If it is, it will check the message type and it can be normal 
message, barrier message and commit messages.
Normal Message: Process the message. 
Barrier Message: tell the tracker the barrier message has been received. 
Commit Message:If it does not have state, just increment the batch ID. 
If yes, it will update the reverse log, incremental log, and incrment the 
batch ID.

**Batch Commit** 

**Batch Recovery**

Batch Recovery is controlled by batch manager. As running of the system, 
the batch manager will detect any possible failure in the systems. Since 
the batch manager sends a barrier every certain time period and the batch
tracker guarantee every message is processed. Therefore, if there are some
messages have not processed by any operators, we can assume failures happens
in these grains. In Orleans, there are machenism that grains will be 
deactived after long-time idle state. So we just active a new grain and load
the data from the previous grain's incremental log. For other grains, batch manager
will broadcast message to them so that they can load data from their reverse log.

**On failures**
1. Detect the failures by finding exception in method call. Stop broadcasting barriers. 
2. Find the failed grains and reactive new grain for each of them. (Impossible because the manager can only get a handle)
3. Let the new grain load their incremental log. 
4. Remove the failure grains from observer list.
5. Broadcast the rollback message to unfailed grain and revert the state from reverse log.
6. Add the new grains into the observer list. 
7. Clear the tracking information in tracker.
8. Clear the reverse log and incremental log(memory) of every grains.
9. Restart the timer of barriers. 

**Reactive Grain Mechanism**
To Recovery a stateful operator:
1. Restart a new grain
2. Rollback the state(statesmap and batchID)
3. Remove the failed from the topology
4. Mark the new grain as restart grain
5. Tell coordinator start recovery 

To recovery a stateless operator:
1. Restart a new grain
2. Rollback the rooting inforamtion
   a. Find the operator settings
   b. Load the setting 
   c. mark as failed, so when it receive recovery message it will revert states by incremental log,
      otherwise it will reverst from reverse log. 
3. Remove the failed from topology
4. Make the new grain as restart 
5. Tell coordinator start recovery

To recovery a source:
Same as the operator, but the source need know the stream

**Orleans Recovery Mechanism**: 

If a grain is not active anymore, Orleans will reactivate it in the next method call. The
thing you need to handle and make sure is correct in the context of your application is the
state. A grain's state can be partially updated or the operation might be something which 
should be done across multiple grains and is carried on partially. 

After you see a grain operation fail you can do one or more of the following.

1. Simply retry your action, especially if it doesn't involve any state changes which might be half done. This is by far the most typical case.
2. Try to fix/reset the partially changed state by calling a method which resets the state to the last known correct state or just reads it from storage by calling ReadStateAsync.
3. Reset the state of all related activations as well to ensure a clean state for all of them.
4. Perform multi-grain state manipulations using a Process Manager or database transaction to make sure it's either done completely or nothing is changed to avoid the state being partially updated.

**Experiments Design**
1. Processing Performance Testing
      a. Throughput = 1/latency
2. Fault Tolerance
3. Scalability & Pipelinging 