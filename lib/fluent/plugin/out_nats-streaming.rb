require "fluent/plugin/output"
require 'stan/client'

module Fluent::Plugin
  class NatsStreamingOutput < Output

    Fluent::Plugin.register_output('nats-streaming', self)

    helpers :formatter, :thread, :inject

    DEFAULT_FORMAT_TYPE = 'json'

    config_param :server, :string, :default => 'localhost:4222',
                 :desc => "NATS streaming server host:port"
    config_param :cluster_id, :string, :default => 'fluentd',
                 :desc => "cluster id"
    config_param :client_id, :string, :default => 'fluentd',
                 :desc => "client id"
    config_param :durable_name, :string, :default => nil,
                 :desc => "durable name"

    config_param :max_reconnect_attempts, :integer, :default => 10,
                 :desc => "The max number of reconnect tries"
    config_param :reconnect_time_wait, :integer, :default => 5,
                 :desc => "The number of seconds to wait between reconnect tries"
    config_param :connect_timeout, :integer, :default => 2,
                 :desc => "Connect timeout in seconds"
    config_param :timeout, :integer, :default => 5,
                 :desc => "Ack timeout"

    config_section :buffer do
      config_set_default :@type, 'memory'
      config_set_default :flush_mode, :interval
      config_set_default :flush_interval, 1
      config_set_default :chunk_keys, ['tag']
      config_set_default :flush_at_shutdown, true
      config_set_default :chunk_limit_size, 10 * 1024
    end

    config_section :format do
      config_set_default :@type, DEFAULT_FORMAT_TYPE
      config_set_default :add_newline, false
    end

    def multi_workers_ready?
      true
    end

    def prefer_buffered_processing
      false
    end

    def formatted_to_msgpack_binary?
      true
    end

    def initialize
      super
      @sc = nil
      @mutex = Mutex.new
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
        max_reconnect_attempts: @max_reconnect_attempts,
        connect_timeout: @connect_timeout
      }

      formatter_conf = conf.elements('format').first || {"@type" => DEFAULT_FORMAT_TYPE }
      #unless formatter_conf
      #  raise Fluent::ConfigError, "<format> section is required."
      #end
      unless formatter_conf["@type"]
        raise Fluent::ConfigError, "format/@type is required."
      end
      @formatter_proc = setup_formatter(formatter_conf)
    end

    def start
      super
      # log.info "start..."
      # thread_create(:nats_streaming_output_main, &method(:run))
      run
    end

    def run
      # log.info "run..."
      @sc = STAN::Client.new

      log.info "connect nats server #{@sc_config[:servers]} #{cluster_id} #{client_id}"
      @sc.connect(@cluster_id, @client_id.gsub(/\./, '_'), nats: @sc_config)
      log.info "connected"

#      while thread_current_running?
#        log.info "test connection"
#        @sc.nats.flush(@reconnect_time_wait)
#        sleep(5)
#      end
    end

    def setup_formatter(conf)
      type = conf['@type']
      case type
      when 'json'
        begin
          require 'oj'
          Oj.default_options = Fluent::DEFAULT_OJ_OPTIONS
          Proc.new { |tag, time, record| Oj.dump(record) }
        rescue LoadError
          require 'yajl'
          Proc.new { |tag, time, record| Yajl::Encoder.encode(record) }
        end
      when 'ltsv'
        require 'ltsv'
        Proc.new { |tag, time, record| LTSV.dump(record) }
      else
        @formatter = formatter_create(usage: 'kafka-plugin', conf: conf)
        @formatter.method(:format)
      end
    end

    def publish(tag, time, record)
      record_buf = @formatter_proc.call(tag, time, record)
      log.trace "Send record: #{record_buf}"

      begin
        @sc.publish(tag, record_buf.b, {timeout: @timeout} )
      rescue Exception => e
        @flag = "stopped"
        log.error e

        @mutex.synchronize do
          if @flag != "stopped"
            break
          end

          begin
            try=0
            # should have enough time for other threads to finish job
            begin
              sleep(@timeout)
              try+=1
              @sc.close if @sc
            rescue
              log.warn "close error #{try}"
              retry if try<3
            end

            log.info "reconnect nats server #{@sc_config[:servers]} #{cluster_id} #{client_id}"
            @sc = STAN::Client.new
            @sc.connect(@cluster_id, @client_id.gsub(/\./, '_'), nats: @sc_config)
          rescue Exception => e2
            log.error e2

            log.info "connect failed, retry..."
            retry
          end

          @flag = "connected"
          log.debug "reconnected"
        end

        retry
      end
    end

    def process(tag, es)
      # log.info "process..."
      # es = inject_values_to_event_stream(tag, es)
      messages = 0
      es.each do |time, record|
        publish(tag, time, record) 
        messages += 1
      end

      if messages > 0
        log.debug { "#{tag}: #{messages} messages send." }
      end
    end

    def write(chunk)
      # log.info "write..."
      return if chunk.empty?
      process(chunk.metadata.tag, chunk)
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
