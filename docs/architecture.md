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
```

## System Topology

```text
                                      +--------------------------------------+
                                      |              Application             |
                                      |--------------------------------------|
                                      | PGWAL                                |
                                      | - owns replication connection pool   |
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
  [4] PGWAL gets a LogicalReplicationConnection from ThreadedConnectionPool
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
| app.consume(consumer_a) -> thread_a                                               |
| app.consume(consumer_b) -> thread_b   # supported structurally, discouraged       |
| app.run()                                                                         |
+--------------------------------------+--------------------------------------------+
                                       |
                                       v
+--------------------------------------+--------------------------------------------+
| Consumer thread N                                                                |
|-----------------------------------------------------------------------------------|
| dedicated LogicalReplicationConnection                                            |
| dedicated replication cursor                                                      |
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
                                  | RabbitPublisher  : daemon thread + pika ioloop + internal queue     |
                                  | KafkaPublisher   : daemon thread + poll/sleep loop + internal queue |
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
         +--> EXIT not set? -------- yes ---> stop consumer and break
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
    |     +--> start daemon thread if not already running
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
| Shutdown control               | shared EXIT threading.Event                         |
+--------------------------------------------------------------------------------------+
```

## Failure / Shutdown Flow

```text
                    +--------------------+
                    | KeyboardInterrupt  |
                    | Exception          |
                    | EXIT.clear()       |
                    +---------+----------+
                              |
                              v
                    +---------+----------+
                    | PGWAL.run() except |
                    +---------+----------+
                              |
          +-------------------+-------------------+
          |                                       |
          v                                       v
+---------+-----------+               +-----------+---------+
| close_pool()        |               | stop_publishers()   |
| close DB conns      |               | flush / close sink  |
+---------+-----------+               +-----------+---------+
          |                                       |
          v                                       v
+---------+-----------+               +-----------+---------+
| consumer loops see  |               | publisher loops see |
| EXIT not set        |               | EXIT not set        |
+---------+-----------+               +-----------+---------+
          |                                       |
          +-------------------+-------------------+
                              |
                              v
                         process exits
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
pgwal/app.py                  -> PGWAL process, pool, thread orchestration
pgwal/consumers.py            -> replication consumption loop
pgwal/interface.py            -> wal2json replication option model
pgwal/events.py               -> global EXIT event
pgwal/publishers/base.py      -> publisher lifecycle + queue mixin
pgwal/publishers/shell.py     -> local logging sink
pgwal/publishers/rabbitmq.py  -> RabbitMQ sink
pgwal/publishers/kafka.py     -> Kafka sink
tests/test_consumers.py       -> feedback + cursor loop behavior
tests/test_rabbitmq_publisher.py
tests/test_kafka_publisher.py -> sink delivery behavior
```
