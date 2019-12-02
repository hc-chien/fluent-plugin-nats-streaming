fluent-plugin-nats-streaming
============================

nats streaming plugin for [fluentd](https://github.com/fluent/fluentd) Event Collector

# Getting Started
example for nats streaming input:

~~~~
  <system>
    workers 2
  </system>

  <source>
    @type nats-streaming
    server "127.0.0.1:4222,127.0.0.1:4223,127.0.0.1:4224"
    cluster_id test-cluster

    client_id "in-#{Socket.gethostname}-#{worker_id}"
    channel   nats.test
    queue     test
  </source>
 
  <match nats.test>
    @type stdout
  </match>
~~~~

example for nats streaming output:

~~~~
  <system>
    workers 2
  </system>

  <match nats.**>
    @type nats-streaming
    server "127.0.0.1:4222,127.0.0.1:4223,127.0.0.1:4224"
    client_id "out-#{Socket.gethostname}-#{worker_id}"
    cluster_id test-cluster

    <format>
      @type json
    </format>
  </match>
~~~~

# Configuration
* **server** (string) (optional): NATS streaming server host:port
  * Default value: `localhost:4222`
* **cluster_id** (string) (optional): cluster id 
  * Default value: `fluentd`
* **client_id** (string) (optional): client id 
  * Default value: `fluentd`
* **durable_name** (string) (optional): durable name
  * Default value: `fluentd`
* **queue** (string) (optional): queue name
  * Default value: `fluentd`
* **channel** (string) : channel name
  * Default value: nil
* **max_reconnect_attempts** (integer) : The max number of reconnect tries
  * Default value: 10
* **reconnect_time_wait** (integer) : The number of seconds to wait between reconnect tries
  * Default value: 5
* **timeout** (integer) : Ack timeout when publish
  * Default value: 5












