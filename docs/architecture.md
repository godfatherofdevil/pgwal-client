# PGWAL Architecture

## Scope

```text
Current-state architecture for this repository.

Focus:
Postgres -> PGWAL -> WALConsumer -> Publisher(s) -> Destination

Implemented destinations:
- Shell logs
- RabbitMQ
- Kafka
```

## Assumptions

```text
1. This doc describes the code as implemented today.
2. WAL decoding uses the Postgres logical replication output plugin: wal2json.
3. One active consumer is the practical production shape, even though the code can start multiple consumers.
4. Fan-out happens inside a consumer by iterating configured publishers.
5. WAL feedback intentionally advances after successful local publisher handoff, not broker confirmation.
```

## System Topology

```text
                                      +--------------------------------------+
                                      |              Application             |
                                      |--------------------------------------|
                                      | PGWAL                                |
                                      | - owns consumer handles              |
                                      | - owns per-app stop state            |
                                      | - owns worker threads                |
                                      | - owns publisher lifecycle           |
                                      +-------------------+------------------+
                                                          |
                                                          | consume(consumer)
                                                          v
                                 +------------------------+------------------------+
                                 |                   WALConsumer                  |
                                 |------------------------------------------------|
                                 | - replication_slot                             |
                                 | - replication_opts (wal2json options)          |
                                 | - publishers[]                                 |
                                 | - consume_async(cursor)                        |
                                 +------------------------+------------------------+
                                                          |
                                                          | start_replication()
                                                          v
+------------------+        WAL records        +----------+-----------+
|  Postgres DB     |==========================>| Logical Replication  |
|------------------|                           | Cursor / Slot        |
| Tables           |                           |----------------------|
| WAL              |<==========================| feedback flush_lsn   |
| wal2json plugin  |       ack / flush_lsn     +----------+-----------+
+------------------+                                      |
                                                          | read_message()
                                                          v
                                 +------------------------+------------------------+
                                 |               ReplicationMessage               |
                                 |------------------------------------------------|
                                 | payload      data_start      send_time         |
                                 +------------------------+------------------------+
                                                          |
                                                          | _consume(msg)
                                                          v
                     +------------------------------------+------------------------------------+
                     |                               Fan-out Loop                              |
                     |-------------------------------------------------------------------------|
                     | for publisher in publishers: publisher.publish(msg)                     |
                     +------------------------+------------------------+------------------------+
                                              |                        |
                                              |                        |
                                              v                        v
                                  +-----------+---------+    +---------+-----------+
                                  | RabbitPublisher     |    | KafkaPublisher     |
                                  |---------------------|    |--------------------|
                                  | queue msg.payload   |    | queue msg.payload  |
                                  | async broker loop   |    | producer loop      |
                                  +-----------+---------+    +---------+----------+
                                              |                        |
                                              v                        v
                                      +-------+------+         +-------+------+
                                      | RabbitMQ     |         | Kafka Topic  |
                                      +--------------+         +--------------+

                                              ^
                                              |
                                  +-----------+---------+
                                  | ShellPublisher      |
                                  |---------------------|
                                  | log payload locally |
                                  +---------------------+
```

## End-to-End Runtime Flow

```text
  [1] Row change in Postgres
       |
       v
  [2] Postgres appends change to WAL
       |
       v
  [3] Logical replication slot exposes WAL stream
       |
       v
  [4] PGWAL opens one dedicated LogicalReplicationConnection per consumer thread
       |
       v
  [5] WALConsumer.start_replication(...)
       - slot_name = replication_slot
       - decode = True
       - options = WALReplicationOpts.model_dump(by_alias=True, ...)
       |
       v
  [6] cursor.read_message()
       |
       v
  [7] ReplicationMessage payload is already decoded by wal2json
       |
       v
  [8] WALConsumer._consume(msg)
       - publisher_1.publish(msg)
       - publisher_2.publish(msg)
       - ...
       |
       v
  [9] msg.cursor.send_feedback(flush_lsn=msg.data_start)
       |
       v
 [10] Postgres can advance replication feedback state
       |
       v
 [11] Destination-specific delivery happens asynchronously or inline
       - ShellPublisher: inline logger.info(...)
       - RabbitPublisher: queue -> pika channel -> exchange -> queue
       - KafkaPublisher: queue -> KafkaProducer.send(topic, bytes)
```

## Sequence Diagram

```text
+----------+     +--------+     +-------------+     +----------------+     +-------------+
| Postgres |     | PGWAL  |     | WALConsumer |     | Publisher(s)   |     | Destination |
+----------+     +--------+     +-------------+     +----------------+     +-------------+
     |               |                |                    |                     |
     | WAL changes    |                |                    |                     |
     |--------------->|                |                    |                     |
     |                | get_conn()     |                    |                     |
     |                |--------------->|                    |                     |
     |                |                | start_replication  |                     |
     |<=============== logical replication slot ===========>|                     |
     |                |                | read_message()      |                     |
     |============== ReplicationMessage ====================>|                     |
     |                |                | publish(msg)        |                     |
     |                |                |-------------------> | queue/log/send      |
     |                |                | publish(msg)        |-------------------->|
     |                |                |-------------------> |                     |
     |                |                | send_feedback(lsn)  |                     |
     |<--------------- flush_lsn ------|                    |                     |
     |                |                | next message        |                     |
```

## Concurrency Model

```text
+-----------------------------------------------------------------------------------+
| Main thread                                                                       |
|-----------------------------------------------------------------------------------|
| app = PGWAL(dsn)                                                                  |
| app.consume(consumer_a) -> consumer_handle_a                                      |
| app.consume(consumer_b) -> consumer_handle_b                                      |
| app.state is AppState                                                             |
| app.run()                                                                         |
+--------------------------------------+--------------------------------------------+
                                       |
                                       v
+--------------------------------------+--------------------------------------------+
| Consumer thread N                                                                |
|-----------------------------------------------------------------------------------|
| dedicated LogicalReplicationConnection                                            |
| dedicated replication cursor                                                      |
| per-consumer stop event + ConsumerState                                           |
| loop: consume_async(cursor)                                                       |
+--------------------------------------+--------------------------------------------+
                                       |
                                       +------------------------------+
                                                                      |
                                                                      v
                                  +-----------------------------------+----------------------------------+
                                  | Publisher execution model                                            |
                                  |----------------------------------------------------------------------|
                                  | ShellPublisher   : no worker thread; logs inline                    |
                                  | RabbitPublisher  : tracked worker thread + pika ioloop + queue      |
                                  | KafkaPublisher   : tracked worker thread + poll/sleep loop + queue  |
                                  | all publishers  : lifecycle tracked with PublisherState             |
                                  +-----------------------------------------------------------------------+
```

## Consumer Internals

```text
consume_async(cursor)
    |
    +--> start_replication(cursor)
    |
    +--> loop
         |
         +--> consumer stop requested? --- yes ---> stop consumer and break
         |
         +--> cursor closed? ------- yes ---> return
         |
         +--> read_message()
         |     |
         |     +--> message found? -- yes --> _consume(msg) --> send_feedback(flush_lsn)
         |     |
         |     +--> no message ------ no ---> wait on select(cursor, timeout)
         |
         +--> repeat
```

## Publisher Delivery Paths

### Shell

```text
ReplicationMessage
    |
    v
ShellPublisher.publish(msg)
    |
    v
logger.info(payload, send_time)
    |
    v
terminal / process logs
```

### RabbitMQ

```text
ReplicationMessage
    |
    v
RabbitPublisher.publish(msg)
    |
    +--> ensure_running()
    |     |
    |     +--> start tracked worker thread if not already running
    |
    +--> msg_queue.put_nowait(msg.payload)
          |
          v
      RabbitPublisher.run()
          |
          +--> connect()
          +--> open_channel()
          +--> declare exchange
          +--> declare queue
          +--> bind queue
          +--> enable publisher confirms
          +--> publish_message() on timer
                    |
                    +--> _get_message()
                    +--> basic_publish(exchange, routing_key, payload)
                    +--> broker ack/nack callback updates delivery state
                    |
                    v
                 RabbitMQ exchange -> queue -> downstream consumers

Shutdown path
    |
    +--> stop()
    |     |
    |     +--> mark publisher stopping
    |     +--> request channel / connection close on pika ioloop thread
    |
    +--> wait_stopped(timeout)
          |
          +--> join tracked worker thread
          +--> return only after RabbitPublisher.run() exits
```

### Kafka

```text
ReplicationMessage
    |
    v
KafkaPublisher.publish(msg)
    |
    +--> ensure_running()
    |     |
    |     +--> start daemon thread if not already running
    |
    +--> msg_queue.put_nowait(msg.payload)
          |
          v
      KafkaPublisher.run()
          |
          +--> _get_message()
          +--> str -> utf8 bytes
          +--> producer.send(topic, message)
          +--> optional flush on stop()
          |
          v
       Kafka topic -> downstream consumers
```

## Configuration Flow

```text
User config
    |
    +--> DSN dict
    |     |
    |     +--> PGWAL(dsn)
    |
    +--> WALReplicationOpts(...)
    |     |
    |     +--> serialized with wal2json option aliases
    |           example:
    |           include_xids      -> include-xids
    |           include_timestamp -> include-timestamp
    |           actions           -> "insert, update, delete, truncate"
    |
    +--> Consumer(replication_slot, replication_opts, publishers)
    |
    +--> Publisher-specific broker/topic/exchange config
```

## Reliability and Control Points

```text
+--------------------------------------------------------------------------------------+
| Control point                  | Current behavior                                    |
|--------------------------------+-----------------------------------------------------|
| Backpressure                   | Publisher-local in-memory queues                    |
| Ordering                       | Preserved per consumer -> per publisher queue loop  |
| Delivery acknowledgement       | Postgres flush_lsn sent after fan-out call returns  |
| RabbitMQ broker confirmation   | supported via confirm_delivery callback             |
| Kafka broker confirmation      | delegated to KafkaProducer.send(...)                |
| Retry / redelivery             | broker reconnect exists for RabbitMQ; limited else  |
| Persistence boundary           | Postgres WAL, then broker-specific durability       |
| App lifecycle                  | per-instance AppState + stop_event                  |
| Consumer lifecycle             | per-instance ConsumerState + stop_event             |
| Publisher lifecycle            | per-instance PublisherState + stop_event            |
+--------------------------------------------------------------------------------------+
```

## Failure / Shutdown Flow

```text
                    +----------------------+
                    | KeyboardInterrupt    |
                    | consumer failure     |
                    | explicit app.stop()  |
                    +----------+-----------+
                               |
                               v
                    +----------+-----------+
                    | PGWAL.stop()         |
                    | app.state=STOPPING   |
                    +----------+-----------+
                               |
          +--------------------+--------------------+
          |                                         |
          v                                         v
+---------+-----------+                 +-----------+-----------+
| stop consumers      |                 | stop_publishers()     |
| set ConsumerState   |                 | set PublisherState    |
| stop_event per slot |                 | stop + wait_stopped   |
+---------+-----------+                 +-----------+-----------+
          |                                         |
          v                                         v
+---------+-----------+                 +-----------+-----------+
| consume_async loop  |                 | worker threads exit   |
| exits and closes    |                 | Kafka/Rabbit cleanup  |
| its own repl conn   |                 | sets STOPPED/FAILED   |
+---------+-----------+                 +-----------+-----------+
          |                                         |
          +--------------------+--------------------+
                               |
                               v
                    +----------+-----------+
                    | PGWAL.close()        |
                    | app.state=STOPPED    |
                    +----------------------+
```

## Practical Deployment Shape

```text
Recommended current shape

    Postgres
       |
       v
   1 replication slot
       |
       v
   1 WALConsumer thread
       |
       +--> ShellPublisher          # optional for debugging
       +--> RabbitPublisher         # queue/event distribution
       +--> KafkaPublisher          # stream/event distribution

Avoid scaling by adding many WALConsumer instances inside one PGWAL process.
Scale fan-out at the publisher/destination layer instead.
```

## File Map

```text
pgwal/app.py                  -> PGWAL app lifecycle, ConsumerHandle, AppState
pgwal/consumers.py            -> replication consumption loop, ConsumerState
pgwal/interface.py            -> wal2json replication option model
pgwal/publishers/base.py      -> publisher lifecycle, PublisherState, worker tracking, queue mixin
pgwal/publishers/shell.py     -> local logging sink
pgwal/publishers/rabbitmq.py  -> RabbitMQ sink + deterministic ioloop shutdown
pgwal/publishers/kafka.py     -> Kafka sink
tests/test_consumers.py       -> feedback + cursor loop behavior
tests/test_rabbitmq_publisher.py
tests/test_kafka_publisher.py -> sink delivery behavior
tests/test_thread_model.py    -> lifecycle enums, isolation, queue behavior
```
