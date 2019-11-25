require "fluent/plugin/output"
# require "nats/client"
require 'stan/client'

module Fluent::Plugin
  class NatsStreamingOutput < Output

    Fluent::Plugin.register_output('nats-streaming', self)

    helpers :formatter, :thread, :inject, :compat_parameters

    DEFAULT_BUFFER_TYPE = "memory"
    DEFAULT_FORMAT_TYPE = 'json'

    config_param :server, :string, :default => 'localhost:4222',
                 :desc => "NATS treaming server host:port"
    config_param :cluster_id, :string, :default => 'fluentd',
                 :desc => "cluster id"
    config_param :client_id, :string, :default => 'fluentd',
                 :desc => "client id"
    config_param :default_topic, :string, :default => nil,
                 :desc => "default topic name"
    config_param :queue, :string, :default => nil,
                 :desc => "queue"
    config_param :durable_name, :string, :default => nil,
                 :desc => "durable name"

    config_param :max_reconnect_attempts, :integer, :default => 150,
                 :desc => "The max number of reconnect tries"
    config_param :reconnect_time_wait, :integer, :default => 2,
                 :desc => "The number of seconds to wait between reconnect tries"

    config_section :buffer do
      config_set_default :chunk_keys, ['tag']
      config_set_default :flush_at_shutdown, true
      config_set_default :chunk_limit_size, 10 * 1024
    end

    config_section :format do
      config_set_default :@type, DEFAULT_FORMAT_TYPE
      config_set_default :add_newline, false
    end

    def initialize
      super

      @sc = nil
    end

    def multi_workers_ready?
      true
    end

    attr_accessor :formatter

    def configure(conf)
      super

      @sc_config = {
          servers: ["nats://#{server}"],
          reconnect_time_wait: @reconnect_time_wait,
          max_reconnect_attempts: @max_reconnect_attempts
        }
      @formatter = formatter_create
    end

    def start
      super
      thread_create(:nats_streaming_output_main, &method(:run))
    end

    def close
      super
      @sc.close if @sc
    end

    def terminate
      super
      @sc = nil
    end

    def run
      begin
        log.info "connect nats server nats://#{server} #{cluster_id} #{client_id}"
        @sc = STAN::Client.new
        @sc.connect(@cluster_id, @client_id, @sc_config)
      rescue Exception => e
        log.warn "Send exception occurred: #{e}"
        log.warn "Exception Backtrace : #{e.backtrace.join("\n")}"
        raise
      end
    end

    def process(tag, es)
      es = inject_values_to_event_stream(tag, es)
      es.each do |time,record|
        @sc.publish(tag, format(tag, time, record))
      end
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      @formatter.format(tag, time, record).chomp + "\n"
    end

  end
end
