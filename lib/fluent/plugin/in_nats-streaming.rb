require "fluent/plugin/input"
require 'stan/client'

module Fluent::Plugin
  class NatsStreamingInput < Input

    Fluent::Plugin.register_input('nats-streaming', self)

    helpers :thread

    config_param :server, :string, :default => 'localhost:4222',
                 :desc => "NATS streaming server host:port"
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

    config_param :max_reconnect_attempts, :integer, :default => 10,
                 :desc => "The max number of reconnect tries"
    config_param :reconnect_time_wait, :integer, :default => 5,
                 :desc => "The number of seconds to wait between reconnect tries"

    def multi_workers_ready?
      true
    end

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

      @sub_opts = {
        queue: @queue,
        durable_name: @durable_name,
        start_at: :first,
        deliver_all_available: true,
        ack_wait: 10,  # seconds
        connect_timeout: 2 # seconds
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
      @sc = STAN::Client.new

      begin
        log.info "connect nats server nats://#{server} #{cluster_id} #{client_id}"
        @sc.connect(@cluster_id, @client_id.gsub(/\./, '_'), nats: @sc_config)
        log.info "connected"
      rescue Exception => e
        log.error "Exception occurred: #{e}"
        run
      end

      log.info "subscribe #{channel} #{queue} #{durable_name}"
      @sc.subscribe(@channel, @sub_opts) do |msg|
        tag = @channel
        begin
          message = JSON.parse(msg.data)
        rescue JSON::ParserError => e
          log.error "Failed parsing JSON #{e.inspect}.  Passing as a normal string"
          message = msg.data
        end
        time = Fluent::Engine.now
        router.emit(tag, time, message || {})
      end

      while thread_current_running?
        begin
          log.trace "test connection"
          @sc.nats.flush(@reconnect_time_wait)
          sleep(5)
        rescue Exception => e
          log.error "Exception occurred: #{e}"
          run
        end
      end
    end
  end
end
