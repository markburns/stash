require 'redis'
require 'redis-namespace'

class Stash
  # Adapter for the Redis data structures server.
  # See http://code.google.com/p/redis/
  class RedisAdapter
    def initialize(config)
      # Convert config keys to symbols
      config = symbolize_config(config)

      raise ArgumentError, "missing 'host' key" unless config[:host]
      config[:port] ||= 6379 # Default Redis port

      @config = config
    end

    # Obtain a connection to Redis
    def connection
      redis = Redis.new @config
      redis = Redis::Namespace.new @config[:namespace], :redis => redis if @config[:namespace]

      begin
        yield redis
      ensure
        redis.quit
      end
    end

    # Set a given key within Redis
    def []=(key, value)
      connection { |redis| redis.set key.to_s, value.to_s }
    end

    # Retrieve a given key from Redis
    def [](key)
      case type(key)
      when "none"   then nil
      when "string" then Stash::String.new key, self
      when "list"   then Stash::List.new   key, self
      else raise "unknown Redis key type: #{key}"
      end
    end

    METHOD_MAPPINGS = {
      :type => :type,
      :get => :get,
      :delete => :del,
      :hash_length => :hlen,
      :hash_delete => :hdel,
      :hash_set => :hset,
      :hash_get => :hget,
      :list_length => :llen,
      :list_range => :lrange,
      :hash_value => :hgetall,
      :hash_delete => :hdel,
      :hash_length => :hlen
    }

    def method_missing method, *args
      mapped_method = METHOD_MAPPINGS[method]
      puts "#{method}, mapped to => #{mapped_method}, #{args}"
      if mapped_method
        return  connection do |redis|
          puts redis.to_s
          debugger
          redis.send mapped_method, *args
        end
      else
        super method, args
      end
    end

    # Push an element onto a list
    def list_push(name, value, side)
      connection do |redis|
        case side
        when :right
          redis.rpush name.to_s, value.to_s
        when :left
          redis.lpush name.to_s, value.to_s
        else raise ArgumentError, "left or right plztks"
        end
      end
    end

    # Pop from a list
    def list_pop(name, side)
      connection do |redis|
        case side
        when :right
          redis.rpop name.to_s
        when :left
          redis.lpop name.to_s
        else raise ArgumentError, "left or right plztks"
        end
      end
    end

    # Blocking pop from a list
    def list_blocking_pop(name, side, timeout = nil)
      connection do |redis|
        timeout ||= 0
        res = case side
              when :left
                redis.blpop name, timeout
              when :right
                redis.brpop name, timeout
              else raise ArgumentError, "left or right plztks"
              end

        return res[1] if res
        raise Stash::TimeoutError, "request timed out"
      end
    end


    #######
    private
    #######

    def symbolize_config(config)
      new_config = {}
      config.each do |k, v|
        new_config[k.to_sym] = v
      end
      new_config
    end
  end
end
