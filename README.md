# bridge

Bridges MQTT brokers and RocketMQ: consumes messages from a source, applies a SQL
(PartiQL) transformation to the payload, and publishes to a sink. Source and sink can
each be MQTT or RocketMQ, so data can flow in either direction.

## Installation

Prerequisites:

- JDK 25
- Maven 3.x

Build:

```sh
mvn package
```

## Usage

1. Define connectors and tasks in `conf/tasks.json` (see the checked-in sample):
   - `connectors` — connection credentials (`id`, `type`: `MQTT` or `RocketMQ`,
     `access_point`, `username`, `password`)
   - `tasks` — each task references connectors by `connector_id` for its source and
     sink, and carries a `sql` PartiQL statement applied to every incoming payload
2. Run the shaded fat jar:

   ```sh
   java -jar target/bridge-1.0-SNAPSHOT.jar [path/to/tasks.json]
   ```

   The config path defaults to `conf/tasks.json`. Each task runs on its own virtual
   thread; stop the process with Ctrl+C for a graceful shutdown.

See [docs/README.md](docs/README.md) for notes on writing task SQL.
