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
      :hash_length => :hlen,
      :authenticate => :auth,
      :asynchrononous_rewrite_append_only_file => :bgrewriteaof,
      :asynchrononous_save => :bgsave,
      :list_blocking_pop_then_push => :brpoplpush, # Pop a value from a list, push it to another list and return it; or block until one is available
      :decrement => :decr,
      :decrement_by => :decrby,
      :delete => :del,
      :discard => :discard,
      :echo => :echo,
      :execute => :exec,
      :exists => :exists,
      :expire => :expire,
      :expire_at => :expireat,
      :flush_all => :flushall,
      :flush_db => :flushdb,
      :get => :get,
      :get_bit => :getbit,
      :get_range => :getrange,
      :get_set => :getset,
      :hash_delete => :hdel,
      :hash_exists => :hexists,
      :hash_get => :hget,
      :hgetall => :hgetall,
      :hincrby => :hincrby,
      :hkeys => :hkeys,
      :hash_len => :hlen,
      :hash_multi_get => :hmget,
      :hash_multi_set => :hmset,
      :hash_set => :hset,
      :hash_setnx => :hsetnx,
      :hash_vals => :hvals,
      :increment => :incr,
      :increment_by => :incrby,
      :info => :info,
      :keys => :keys,
      :last_save => :lastsave,
      :lindex => :lindex,
      :linsert => :linsert,
      :llen => :llen,
      :lpop => :lpop,
      :lpush => :lpush,
      :lpushx => :lpushx,
      :lrange => :lrange,
      :lrem => :lrem,
      :lset => :lset,
      :ltrim => :ltrim,
      :mget => :mget,
      :monitor => :monitor,
      :move => :move,
      :mset => :mset,
      :msetnx => :msetnx,
      :multi => :multi,
      :object => :object,
      :persist => :persist,
      :ping => :ping,
      :psubscribe => :psubscribe,
      :publish => :publish,
      :punsubscribe => :punsubscribe,
      :quit => :quit,
      :randomkey => :randomkey,
      :rename => :rename,
      :renamenx => :renamenx,
      :rpop => :rpop,
      :rpoplpush => :rpoplpush,
      :rpush => :rpush,
      :rpushx => :rpushx,
      :sadd => :sadd,
      :save => :save,
      :scard => :scard,
      :sdiff => :sdiff,
      :sdiffstore => :sdiffstore,
      :select => :select,
      :set => :set,
      :setbit => :setbit,
      :setex => :setex,
      :setnx => :setnx,
      :setrange => :setrange,
      :shutdown => :shutdown,
      :sinter => :sinter,
      :sinterstore => :sinterstore,
      :sismember => :sismember,
      :slaveof => :slaveof,
      :slowlog => :slowlog,
      :smembers => :smembers,
      :smove => :smove,
      :sort => :sort,
      :spop => :spop,
      :srandmember => :srandmember,
      :srem => :srem,
      :strlen => :strlen,
      :subscribe => :subscribe,
      :sunion => :sunion,
      :sunionstore => :sunionstore,
      :sync => :sync,
      :ttl => :ttl,
      :type => :type,
      :unsubscribe => :unsubscribe,
      :unwatch => :unwatch,
      :watch => :watch,
      :zadd => :zadd,
      :zcard => :zcard,
      :zcount => :zcount,
      :zincrby => :zincrby,
      :zinterstore => :zinterstore,
      :zrange => :zrange,
      :zrangebyscore => :zrangebyscore,
      :zrank => :zrank,
      :zrem => :zrem,
      :zremrangebyrank => :zremrangebyrank,
      :zremrangebyscore => :zremrangebyscore,
      :zrevrange => :zrevrange,
      :zrevrangebyscore => :zrevrangebyscore,
      :zrevrank => :zrevrank,
      :zscore => :zscore,
      :zunionstore => :zunionstore
       }



=begin
CONFIG GET parameter Get the value of a configuration parameter
CONFIG SET parameter value Set a configuration parameter to the given value
CONFIG RESETSTAT Reset the stats returned by INFO
DBSIZE Return the number of keys in the selected database
DEBUG OBJECT key Get debugging information about a key DEBUG SEGFAULT Make the server crash
DECR key Decrement the integer value of a key by one
DECRBY key decrement Decrement the integer value of a key by the given number
DEL key [key ...] Delete a key
DISCARD Discard all commands issued after MULTI
ECHO message Echo the given string
EXEC Execute all commands issued after MULTI
EXISTS key Determine if a key exists
EXPIRE key seconds Set a key's time to live in seconds
EXPIREAT key timestamp Set the expiration for a key as a UNIX timestamp
FLUSHALL Remove all keys from all databases
FLUSHDB Remove all keys from the current database
GET key Get the value of a key
GETBIT key offset Returns the bit value at offset in the string value stored at key
GETRANGE key start end Get a substring of the string stored at a key
GETSET key value Set the string value of a key and return its old value
HDEL key field [field ...] Delete one or more hash fields
HEXISTS key field Determine if a hash field exists
HGET key field Get the value of a hash field
HGETALL key Get all the fields and values in a hash
HINCRBY key field increment Increment the integer value of a hash field by the given number
HKEYS key Get all the fields in a hash
HLEN key Get the number of fields in a hash
HMGET key field [field ...] Get the values of all the given hash fields
HMSET key field value [field value ...] Set multiple hash fields to multiple values
HSET key field value Set the string value of a hash field
HSETNX key field value Set the value of a hash field, only if the field does not exist
HVALS key Get all the values in a hash
INCR key Increment the integer value of a key by one
INCRBY key increment Increment the integer value of a key by the given number
INFO Get information and statistics about the server
KEYS pattern Find all keys matching the given pattern
LASTSAVE Get the UNIX time stamp of the last successful save to disk
LINDEX key index Get an element from a list by its index
LINSERT key BEFORE|AFTER pivot value Insert an element before or after another element in a list
LLEN key Get the length of a list
LPOP key Remove and get the first element in a list
LPUSH key value [value ...] Prepend one or multiple values to a list
LPUSHX key value Prepend a value to a list, only if the list exists
LRANGE key start stop Get a range of elements from a list
LREM key count value Remove elements from a list
LSET key index value Set the value of an element in a list by its index
LTRIM key start stop Trim a list to the specified range
MGET key [key ...] Get the values of all the given keys
MONITOR Listen for all requests received by the server in real time
MOVE key db Move a key to another database
MSET key value [key value ...] Set multiple keys to multiple values
MSETNX key value [key value ...] Set multiple keys to multiple values, only if none of the keys exist
MULTI Mark the start of a transaction block
OBJECT subcommand [arguments [arguments ...]] Inspect the internals of Redis objects
PERSIST key Remove the expiration from a key
PING Ping the server
PSUBSCRIBE pattern [pattern ...] Listen for messages published to channels matching the given patterns
PUBLISH channel message Post a message to a channel
PUNSUBSCRIBE [pattern [pattern ...]] Stop listening for messages posted to channels matching the given patterns
QUIT Close the connection
RANDOMKEY Return a random key from the keyspace
RENAME key newkey Rename a key
RENAMENX key newkey Rename a key, only if the new key does not exist
RPOP key Remove and get the last element in a list
RPOPLPUSH source destination Remove the last element in a list, append it to another list and return it
RPUSH key value [value ...] Append one or multiple values to a list
RPUSHX key value Append a value to a list, only if the list exists
SADD key member [member ...] Add one or more members to a set
SAVE Synchronously save the dataset to disk
SCARD key Get the number of members in a set
SDIFF key [key ...] Subtract multiple sets
SDIFFSTORE destination key [key ...] Subtract multiple sets and store the resulting set in a key
SELECT index Change the selected database for the current connection
SET key value Set the string value of a key
SETBIT key offset value Sets or clears the bit at offset in the string value stored at key
SETEX key seconds value Set the value and expiration of a key
SETNX key value Set the value of a key, only if the key does not exist
SETRANGE key offset value Overwrite part of a string at key starting at the specified offset
SHUTDOWN Synchronously save the dataset to disk and then shut down the server
SINTER key [key ...] Intersect multiple sets
SINTERSTORE destination key [key ...] Intersect multiple sets and store the resulting set in a key
SISMEMBER key member Determine if a given value is a member of a set
SLAVEOF host port Make the server a slave of another instance, or promote it as master
SLOWLOG subcommand [argument] Manages the Redis slow queries log
SMEMBERS key Get all the members in a set
SMOVE source destination member Move a member from one set to another
SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination] Sort the elements in a list, set or sorted set
SPOP key Remove and return a random member from a set
SRANDMEMBER key Get a random member from a set
SREM key member [member ...] Remove one or more members from a set
STRLEN key Get the length of the value stored in a key
SUBSCRIBE channel [channel ...] Listen for messages published to the given channels
SUNION key [key ...] Add multiple sets
SUNIONSTORE destination key [key ...] Add multiple sets and store the resulting set in a key
SYNC Internal command used for replication
TTL key Get the time to live for a key
TYPE key Determine the type stored at key
UNSUBSCRIBE [channel [channel ...]] Stop listening for messages posted to the given channels
UNWATCH Forget about all watched keys
WATCH key [key ...] Watch the given keys to determine execution of the MULTI/EXEC block
ZADD key score member [score] [member] Add one or more members to a sorted set, or update its score if it already exists
ZCARD key Get the number of members in a sorted set
ZCOUNT key min max Count the members in a sorted set with scores within the given values
ZINCRBY key increment member Increment the score of a member in a sorted set
ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] Intersect multiple sorted sets and store the resulting sorted set in a new key
ZRANGE key start stop [WITHSCORES] Return a range of members in a sorted set, by index
ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count] Return a range of members in a sorted set, by score
ZRANK key member Determine the index of a member in a sorted set
ZREM key member [member ...] Remove one or more members from a sorted set
ZREMRANGEBYRANK key start stop Remove all members in a sorted set within the given indexes
ZREMRANGEBYSCORE key min max Remove all members in a sorted set within the given scores
ZREVRANGE key start stop [WITHSCORES] Return a range of members in a sorted set, by index, with scores ordered from high to low
ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count] Return a range of members in a sorted set, by score, with scores ordered from high to low
ZREVRANK key member Determine the index of a member in a sorted set, with scores ordered from high to low
ZSCORE key member Get the score associated with the given member in a sorted set
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] Add multiple sorted sets and store the resulting sorted set in a new key
=end
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
