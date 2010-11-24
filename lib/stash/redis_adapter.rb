require 'redis'
require 'redis-namespace'

class Stash
  # Adapter for the Redis data structures server. 
  # See http://code.google.com/p/redis/
  class RedisAdapter
    attr_reader :capabilities
    
    def initialize(config)
      # Symbolize keys in config
      config = config.inject({}) { |h, (k, v)| h[k.to_sym] = v; h }
      
      raise ArgumentError, "missing 'host' key" unless config[:host]
      config[:port] ||= 6379 # Default Redis port
      
      redis = Redis.new config
      redis = Redis::Namespace.new config[:namespace], :redis => redis if config[:namespace]
      
      @capabilities = [:string]
      
      # Redis 2.0RC+ supports blocking pop
      @capabilities << :bpop if redis.info['redis_version'] >= "1.3.0"
      
      @redis = redis
    end
    
    # Set a given key within Redis
    def []=(key, value)
      @redis.set key.to_s, value.to_s
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
    
    # Retrieve the type for a given key
    def type(key)
      @redis.type key.to_s
    end
    
    # Retrieve a key as a string
    def get(key)
      @redis.get key.to_s
    end
    
    # Delete a key
    def delete(key)
      @redis.del key.to_s
    end
    
    # Push an element onto a list
    def list_push(name, value, side)
      case side
      when :right
        @redis.rpush name.to_s, value.to_s
      when :left
        @redis.lpush name.to_s, value.to_s
      else raise ArgumentError, "left or right plztks"
      end
    end
        
    # Pop from a list
    def list_pop(name, side)
      case side
      when :right
        @redis.rpop name.to_s
      when :left
        @redis.lpop name.to_s
      else raise ArgumentError, "left or right plztks"
      end
    end
    
    # Blocking pop from a list
    def list_blocking_pop(name, side, timeout = nil)
      timeout ||= 0
      res = case side
      when :left
        @redis.blpop name, timeout
      when :right
        @redis.brpop name, timeout
      else raise ArgumentError, "left or right plztks"
      end
      
      return res[1] if res
      raise Stash::TimeoutError, "request timed out"
    end
    
    # Retrieve the length of a list
    def list_length(name)
      @redis.llen name.to_s
    end
    
    # Retrieve the given range from a list
    def list_range(name, from, to)
      @redis.lrange name.to_s, from, to
    end
  end
end