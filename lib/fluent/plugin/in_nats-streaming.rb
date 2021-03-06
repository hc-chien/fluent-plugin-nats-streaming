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
    config_param :durable_name, :string, :default => nil,
                 :desc => "durable name"
    config_param :queue, :string, :default => nil,
                 :desc => "queue name"
    config_param :channel, :string, :default => nil,
                 :desc => "channel name"
    config_param :start_at, :string, :default => "deliver_all_available",
                 :desc => "start at"

    config_param :max_reconnect_attempts, :integer, :default => 10,
                 :desc => "The max number of reconnect tries"
    config_param :reconnect_time_wait, :integer, :default => 5,
                 :desc => "The number of seconds to wait between reconnect tries"
    config_param :max_consume_interval, :integer, :default => 120,
                 :desc => "max consume interval time"

    def multi_workers_ready?
      true
    end

    def initialize
      super
      @sc = nil
      @mutex = Mutex.new
      @last = 0
    end

    def configure(conf)
      super

      servers = [ ]
      @server.split(',').map do |server_str|
        server_str = server_str.strip
        servers.push("nats://#{server_str}")
      end

      @sc_config = {
        servers: servers,
        reconnect_time_wait: @reconnect_time_wait,
        max_reconnect_attempts: @max_reconnect_attempts
      }

      @sub_opts = {
        queue: @queue,
        durable_name: @durable_name,
        start_at: @start_at.to_sym,
        deliver_all_available: true,
        ack_wait: 10,  # seconds
        connect_timeout: 2 # seconds
      }
    end

    def start
      super
      thread_create(:nats_streaming_input_main, &method(:run))
    end

    def reconnect

      @mutex.synchronize do
        begin
          try = 0
          begin
            sleep(5)
            try+=1
            @sc.close if @sc
          rescue
            log.warn "close error #{try}"
            retry if try<3
          end

          client_id= @client_id.gsub(/\./, '_')
          client_id += rand(10000).to_s if @queue || @durable_name
          log.info "connect nats server #{@sc_config[:servers]} #{cluster_id} #{client_id}"

          @sc = STAN::Client.new
          @sc.connect(@cluster_id, client_id, nats: @sc_config)
        rescue Exception => e
          log.error e

          log.info "connect failed, retry..."
          retry
        end

        log.debug "connected"
        @last = Time.now.to_i

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
          @last = time.to_i
        end
      end
    end

    def run
      # reconnect

      while thread_current_running?
        # log.trace "test connection"
        # @sc.nats.flush(@reconnect_time_wait)
        # raise "wait too long for next message.." if (@max_consume_interval > 0 and Time.now.to_i - @last > @max_consume_interval)
        reconnect if (@max_consume_interval > 0 and Time.now.to_i - @last > @max_consume_interval)
        sleep(5)
      end
    end

    def close
      super
      @sc.close if @sc
    end

    def terminate
      super
      @sc = nil
    end
  end
end
