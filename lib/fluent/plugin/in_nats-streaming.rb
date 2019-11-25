require "fluent/plugin/input"
require 'stan/client'

module Fluent::Plugin
  class NatsStreamingInput < Input

    Fluent::Plugin.register_input('nats-streaming', self)

    helpers :thread

    DEFAULT_FORMAT_TYPE = 'json'

    config_param :server, :string, :default => 'localhost:4222',
                 :desc => "NATS treaming server host:port"
    config_param :cluster_id, :string, :default => 'fluentd',
                 :desc => "cluster id"
    config_param :client_id, :string, :default => 'fluentd',
                 :desc => "client id"
    config_param :durable_name, :string, :default => 'fluentd',
                 :desc => "durable name"
    config_param :queue, :string, :default => 'fluentd',
                 :desc => "queue name"
    config_param :channel, :string, :default => nil,
                 :desc => "channel name"

    config_param :max_reconnect_attempts, :integer, :default => 150,
                 :desc => "The max number of reconnect tries"
    config_param :reconnect_time_wait, :integer, :default => 2,
                 :desc => "The number of seconds to wait between reconnect tries"

    def initialize
      super

      @sc = nil
    end

    def configure(conf)
      super

      @sc_config = {
          servers: ["nats://#{server}"],
          reconnect_time_wait: @reconnect_time_wait,
          max_reconnect_attempts: @max_reconnect_attempts
        }
    end

    def start
      super
      thread_create(:nats_streaming_input_main, &method(:run))
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
        # time = Fluent::Engine.now
        log.info "connect nats server nats://#{server} #{cluster_id} #{client_id}"
        @sc = STAN::Client.new
        @sc.connect(@cluster_id, @client_id, @sc_config)
      rescue Exception => e
        log.warn "Send exception occurred: #{e}"
        log.warn "Exception Backtrace : #{e.backtrace.join("\n")}"
        raise
      end

      log.info "subscribe #{channel} #{queue} #{durable_name}"
      @sc.subscribe(@channel, queue: @queue, durable_name: @durable_name) do |msg|
        tag = @channel
        begin
          message = JSON.parse(msg.data)
        rescue JSON::ParserError => e
          log.error "Failed parsing JSON #{e.inspect}.  Passing as a normal string"
          message = msg.data
        end
        time = Fluent::Engine.now
        # log.warn "Emit  #{tag} #{message}"
        router.emit(tag, time, message || {})
      end
    end
  end
end
