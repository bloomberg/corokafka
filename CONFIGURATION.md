# Configuration

The following configuration options are complementary to the RdKafka [options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) and affect exclusively the functionality of this library.
          
## Connector configuration

| Property | Range | Default | Description |
| :------- | :---: | :-----: | :---------- |
| internal.connector.poll.interval.ms | \> 0 | 100 | Set the internal poll interval for consumers and producers. Polling of all registered consumers and producers is done in parallel at each interval. |
| internal.connector.max.payload.output.length | \>=-1 | 100 | Sets the length (bytes) of the serialized message to be printed when an internal error occurs. -1 prints the entire message. | 
| internal.connector.shutdown.io.wait.timeout.ms | \>=0 | 2000 | How long to wait for outstanding IO operations on shutdown. |
---

## Consumer configuration

| Property | Range | Default | Description |
| :------- | :---: | :-----: | :---------- |
| internal.consumer.pause.on.start | true, false | false | Start the consumer in paused state. User needs to call `resume()` to consume. |
| internal.consumer.timeout.ms | \>= -1 | 1000 | Sets the timeout on any operations requiring a timeout such as poll, query offsets, etc. Set to **-1** for infinite timeout. |
| internal.consumer.poll.timeout.ms | \>= -1 | N/A | If set, overrides the 'internal.consumer.timeout.ms' default setting for polling only. Set to **-1** for infinite timeout. |
| internal.consumer.auto.offset.persist | true, false | false | Enables auto-commit/auto-store inside the `ReceivedMessage` destructor. |
| internal.consumer.auto.offset.persist.on.exception | true, false | false | Dictates if the offset persist should be aborted as a result of an exception. This could allow the application to reprocess a message following an exception. This is only valid if **internal.consumer.auto.offset.persist=true**. |
| internal.consumer.offset.persist.strategy | commit, store | store | Determines if offsets are committed or stored locally. Some rdkafka settings will be changed according to **note 2** below. If **store** is chosen, **auto.commit.interval.ms > 0** must be set. |
| internal.consumer.commit.exec | sync, async | async | Dictates if offset commits should be synchronous or asynchronous. |
| internal.consumer.commit.num.retries | \>= 0 | MAX_UINT | Sets the number of times to retry committing an offset before giving up. |
| internal.consumer.commit.backoff.strategy | linear, exponential | linear | Back-off strategy when initial commit fails. |
| internal.consumer.commit.backoff.interval.ms | \> 0 | 100 | Time in ms between retries. |
| internal.consumer.commit.max.backoff.ms | \>= **internal.consumer.commit.backoff.interval.ms** | 1000 | Maximum back-off time for retries. If set, this has higher precedence than **internal.consumer.commit.num.retries**. |
| internal.consumer.poll.strategy | batch, serial, roundrobin | serial | Determines how messages are read from rdkafka queues. The **batch** strategy is to read the entire batch of messages from the main consumer queue and is more _performant_. If **roundrobin** strategy is selected, messages are read in round-robin fashion from each partition at a time. In this mode, the **timeout.ms** and **poll.timeout.ms** are divided by the **read.size** up to a minimum of **roundrobin.min.timeout.ms**. |
| internal.consumer.roundrobin.min.poll.timeout.ms | \> 0 | 10 | Minimum poll timeout when consuming messages in **roundrobin** mode. |
| internal.consumer.read.size | -1, \> 0 | 100 | Number of messages to read on each poll interval. In **roundrobin** or **serial** mode, setting this to -1 will block the **internal.consumer.poll.io.thread.id** thread exclusively for polling until the Consumer is destroyed. See `ConnectorConfiguration::setPollInterval()` for more details.|
| internal.consumer.message.prefetch | true, false | false | If **internal.consumer.poll.strategy=batch**, start pre-fetching the next series of messages while processing the current ones. This increases performance but may cause additional burden on the broker. |
| internal.consumer.receive.callback.thread.range.low | [0, \<quantum_IO_threads\>) | 0 | Specifies the lowest thread id on which receive callbacks will be called. See **note 1** below for more details. |
| internal.consumer.receive.callback.thread.range.high | [0, \<quantum_IO_threads\>) | \<quantum_IO_threads\> - 1 | Specifies the highest thread id on which receive callbacks will be called. See **note 1** below for more details. This setting is only valid if **internal.consumer.receive.invoke.thread=io**. |
| internal.consumer.receive.callback.exec | sync, async | async | Only applies when **internal.consumer.receive.invoke.thread=io**. Call the receiver callback asynchronously. When set to **async** all received messages from a poll interval (see **internal.consumer.read.size** above) are enqueued on the IO threads into the specified thread range (see above) and the receiver callback is called asynchronously, which means that another poll operation may start immediately. When set to **sync**, corokafka will deliver each message synchronously into the specified thread range (see above) and will wait until the entire batch is delivered to the application, before making another poll operation. In the synchronous case, it's recommended not to do too much processing inside the receiver callback since it delays the delivery of other messages and to dispatch the message to some other work queue as soon as possible (see **note 1** for more details). |
| internal.consumer.receive.invoke.thread | io, coro | io | Determines where the receiver callback will be invoked from. In the coroutine case, the execution mode will be forced to sync (i.e. **internal.consumer.receive.callback.exec=sync**) since **async** execution has the risk of delivering messages from two consecutive poll intervals out of order. Receiving messages inside a coroutine could increase performance if the callbacks are non-blocking (or if they block for very short intervals). When invoking on coroutines, the receiver callback will always be called serially for all messages inside a particular poll batch. |
| internal.consumer.log.level | emergency, alert, critical, error, warning, notice, info, debug | info | Sets the log level for this consumer. Note that if the rdkafka **debug** property is set, **internal.producer.log.level** will be automatically adjusted to **debug**.|
| internal.consumer.skip.unknown.headers | true, false | true | If unknown headers are encountered (i.e. for which there is no registered deserializer), they will be skipped. If set to **false**, an error will be thrown. |
| internal.consumer.auto.throttle | true, false | false | When enabled, the consumers will be automatically paused/resumed when throttled by the broker.|
| internal.consumer.auto.throttle.multiplier | \>= 1 | 1 | Change this value to pause the consumer by **\<throttle time\> x multiplier** ms instead. This only works if **internal.consumer.auto.throttle=true**. |
| internal.consumer.preprocess.messages | true, false | true | Enable the preprocessor callback if it has been registered. Otherwise it can be enabled/disabled via `ConsumerManager::preprocess()`. |
| internal.consumer.poll.io.thread.id | [-1, \<quantum_io_threads\>) | -1 | Specify a dedicated IO thread to run all poll operations. This ensures the poll operation will not be delayed by other consurrent IO operations. If set to -1, the poll operation will be run on any thread. |
| internal.consumer.process.coro.thread.id | [-1, \<quantum_coro_threads\>) | -1 | Specify a dedicated coroutine thread to execute message processing tasks. Set to -1 to use any coroutine. |
| internal.consumer.preserve.message.order | true, false | false | Set to **true** if strict message order must be preserved and messages belonging to the same partition will be delivered on the same thread. |

---

## Producer configuration

| Property | Range | Default | Description |
| :------- | :---: | :-----: | :---------- |
| internal.producer.timeout.ms | \>= -1 | 1000 | Sets the timeout on poll and flush operations. Set to **-1** for infinite timeout. |
| internal.producer.retries | \>= 0 | 0 | Sets the number of times to retry sending a message from the internal queue before giving up. Note that this setting is independent of the rdkafka setting **message.send.max.retries**. This setting applies to each individual message and not to entire batches of messages like **message.send.max.retries** does. |
| internal.producer.payload.policy | passthrough, copy | copy | Sets the payload policy on the producer. For sync producers, use **passthrough** for maximum performance. |
| internal.producer.preserve.message.order | true, false | false | Set to **true** if strict message order must be preserved. When setting this to **true**, the following RdKafka options will also be set as **max.in.flight=1** and **message.send.max.retries=1** to avoid possible reordering of packets, unless **enable.idempotence=true** or unless the application explicitly overwrites them. Messages will be buffered and sent sequentially, waiting for broker acks before the next one is processed. This ensures delivery guarantee at the cost of performance. The internal buffer queue size can be checked via `ProducerMetadata::getInternalQueueLength()`. An intermediate, solution with better performance would be to set this setting to **false** and set **max.in.flight=1** which will skip internal buffering altogether. This will guarantee ordering but not delivery. This setting is somewhat redundant to **enable.idempotence** but offers more flexibility, so both should not be sent concurrently. |
| internal.producer.max.queue.length | \> -1 | -1 | Maximum size of the internal queue when producing asynchronously with strict order preservation. Set to -1 to disregard. |
| internal.producer.wait.for.acks.timeout.ms | \>= -1 | N/A | If set, the producer will wait up to the timeout specified when producing a message via `send()`. Otherwise the default timeout **internal.producer.timeout.ms** shall be used. Set to **-1** for infinite timeout. |
| internal.producer.flush.wait.for.acks.timeout.ms | \>= -1 | N/A | If set, the background poll will wait up to the timeout specified when flushing the transmit queue. Otherwise the default timeout **internal.producer.timeout.ms** shall be used. Set to **-1** for infinite timeout.  |
| internal.producer.log.level | emergency, alert, critical, error, warning, notice, info, debug | info | Sets the log level for this producer. |
| internal.producer.auto.throttle | true, false | false | When enabled, the producers will be automatically paused/resumed when throttled by broker. |
| internal.producer.auto.throttle.multiplier | \>= 1 | 1 | Change this value to pause the producer by **\<throttle time\> x multiplier** ms instead. This only works if **internal.producer.auto.throttle=true**. |
| internal.producer.queue.full.notification | edgeTriggered, oncePerMessage, eachOccurence | oncePerMessage | When registering a `QueueFullCallback`, this setting determines when the callback will be raised. If set to **edgeTriggered**, the callback will be raised once after which the application must reset the edge via `ProducerManager::resetQueueFullTrigger()` in order to re-enable it again. |
| internal.producer.poll.io.thread.id | \>=-1 | max io threads - 1 | -1 | Specify a dedicated IO thread to run all poll operations. This ensures the poll operation will not be delayed by other consurrent IO operations. If set to -1, the poll operation will be run on any thread. |
| internal.producer.sync.producer.thread.range.low | [0, \<quantum_io_threads\>) | 0 | When producing synchronously from a coroutine, use this to specify on which IO thread to produce. |
| internal.producer.sync.producer.thread.range.high | [0, \<quantum_io_threads\>) | \<quantum_IO_threads\> - 1 | When producing synchronously from a coroutine, use this to specify on which IO thread to produce. |
---

##### Notes

[1] 

Mapping partitions unto IO threads for the receive callback ensures a maximum amount of parallelization while maintaining arrival order. These two variables allow to manually specify how to distribute **P** partitions belonging to a topic unto **T** IO threads when the receive callback is invoked. The default `[low,high]` values are `[0,T-1]` where T is the **numIoThreads** value used in the **Quantum** dispatcher constructor, and the distribution model is **partition id % num threads** within the `[low,high]` range. This ensures that processing order for all messages is preserved. If a partition reassignment happens, the range remains unchanged and the new partitions will follow the same distribution mechanism. 

Ex: _There are 8 IO threads and 20 partitions assigned to this consumer. We set `[low,high]` range to `[4,7]` which means that the 20 partitions will be mapped unto 4 thread ids `{4,5,6,7}`. When message from partition 10 arrives, the thread id on which the receive callback will be invoked can be calculated as follows: 10 % (7-4+1) + 4 = 6_.
  
[2]
  
**internal.consumer.offset.persist.strategy=commit**:

`enable.auto.commit=false`

`enable.auto.offset.store=false`

`auto.commit.interval.ms=0`
      
**internal.consumer.offset.persist.strategy=store**:

`enable.auto.commit=true`

`enable.auto.offset.store=false`
