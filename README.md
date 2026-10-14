# go-confluent-kafka

A simple Apache Kafka **producer** and **consumer** written in Go using the [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) library.

- **Producer** sends 10 random purchase events (key = user name, value = item name) to the `purchases` topic.
- **Consumer** reads every event from the `purchases` topic and prints it to the terminal until stopped with `Ctrl+C`.

## Project Structure

```
.
├── consumer/
│   └── main.go     # reads events from the "purchases" topic
├── producer/
│   └── main.go     # sends 10 random events to the "purchases" topic
├── go.mod
└── go.sum
```

The producer and consumer live in separate folders on purpose. If both were in the same folder with `package main`, Go would fail with `main redeclared in this block`.

## Prerequisites

- Go 1.26 or later
- Kafka running at `127.0.0.1:9092`

Example of running Kafka locally with Docker:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

## Running

Start the consumer in the first terminal:

```bash
go run ./consumer
```

Then start the producer in a second terminal:

```bash
go run ./producer
```

## Example Output

**Producer:**

```
Produced event to topic purchases: key = htanaka    value = batteries
Produced event to topic purchases: key = awalther   value = alarm clock
Produced event to topic purchases: key = eabara     value = t-shirts
Produced event to topic purchases: key = jsmith     value = book
...
```

**Consumer:**

```
Consumed event from topic purchases: key =            value = halo
Consumed event from topic purchases: key = user-1     value = {"item":"buku","harga":50000}
Consumed event from topic purchases: key = eabara     value = batteries
Consumed event from topic purchases: key = awalther   value = gift card
...
```

## Configuration

All settings are defined as constants at the top of each `main.go`.

| Component | Setting             | Value                      |
| --------- | ------------------- | -------------------------- |
| Producer  | `bootstrap.servers` | `127.0.0.1:9092`           |
| Producer  | `acks`              | `all`                      |
| Producer  | `messageCount`      | `10`                       |
| Producer  | `flushTimeout`      | `15s`                      |
| Consumer  | `bootstrap.servers` | `127.0.0.1:9092`           |
| Consumer  | `group.id`          | `kafka-go-getting-started` |
| Consumer  | `auto.offset.reset` | `earliest`                 |
| Consumer  | `pollTimeout`       | `100ms`                    |

Because `auto.offset.reset` is `earliest`, a new consumer group reads the topic from the very first message. When restarted with the same `group.id`, the consumer resumes from its last committed offset.

## Notes

### Messages with and without a key

The consumer reads **all** messages in the topic, whether or not they have a key. Keys are optional and determine the partition: messages with the same key always go to the same partition, so their order is preserved per key. Messages without a key are printed with an empty key column.

To process only messages that have a key, add this check inside the consumer loop:

```go
if len(ev.Key) == 0 {
    continue
}
```

### `Connect to ipv6#[::1]:9092 failed` error

This error appears when the broker advertises itself as `localhost:9092`. On macOS, `localhost` is tried over IPv6 (`::1`) first, while Kafka only listens on IPv4. The client falls back to IPv4 automatically, so the error is safe to ignore. To get rid of it, add this property to both the producer and consumer `ConfigMap`:

```go
"broker.address.family": "v4",
```
