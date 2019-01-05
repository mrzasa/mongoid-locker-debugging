require 'mongo'
require 'securerandom'
require 'pry'
require 'awesome_print'
::Mongo::Monitoring::CommandLogSubscriber::LOG_STRING_LIMIT = 2_000

module Raw
  class Actions
    def initialize(runner)
      @runner = runner
    end

    def db
      @db ||= @runner.get_db_connection
    end

    def create_wallet
      db[:wallets].insert_one({balance: 0, data: []}).inserted_id
    end

    def create_transaction(wallet_id, amount)
      STDERR.puts ["before transaction", db[:wallets].find(_id: wallet_id).first].to_s.yellow
      add_transaction(wallet_id, amount)
      increment_counter(wallet_id)
    end

    def add_transaction(wallet_id, amount)
      acquire_lock(wallet_id)
      $lock_counter = $lock_counter + 1
      STDERR.puts ["BEGIN", $lock_counter].to_s.blue

      wallet_id = BSON::ObjectId(wallet_id)
      data = db[:wallets].find(_id: wallet_id).first
      balance = data[:balance]
      STDERR.puts ["creating transaction", db[:wallets].find(_id: wallet_id).first].to_s.yellow
      db[:transactions].insert_one(wallet_id: wallet_id, amount: amount)
      db[:wallets].update_one({_id: wallet_id}, { '$set' => { balance: balance + amount } })

      STDERR.puts ["END", $lock_counter].to_s.blue
    ensure
      release_lock(wallet_id)
      STDERR.puts ["lock released", db[:wallets].find(_id: wallet_id).first].to_s.yellow
    end

    def increment_counter(wallet_id)
      data = db[:wallets].find(_id: wallet_id).first
      STDERR.puts ["before incrementing", db[:wallets].find(_id: wallet_id).first].to_s.yellow
      res = db[:wallets].update_one({_id: wallet_id}, {"$inc" => {counter: 1}})
      fail "Increment counter failed" unless res.ok?
      STDERR.puts ["incremented", db[:wallets].find(_id: wallet_id).first].to_s.yellow
    end

    def acquire_lock(id)
      time = db.command({ serverStatus: 1 }).to_a.first["localTime"] # Time.now
      expiration = time + 5
      retry_limit = 500
      retry_count = 0
      sleep_time = 0.5
      loop do

        locking_result = db[:wallets].update_one(
          {
            :_id => id,
            '$or' => [
              # not locked
              { locked_until: nil },
              # expired
              { locked_until: { '$lte' => time } }
            ]
          },

          '$set' => {
            locked_at:    time,
            locked_until: expiration
          }
        )
        acquired_lock = locking_result.ok? && locking_result.documents.first['n'] == 1
        retry_count += 1
        break if acquired_lock
        fail "Update failed on acquiring lock" unless locking_result.ok?
        fail "Cannot acquire lock" if retry_count == retry_limit

        sleep(sleep_time)
      end
    end

    def release_lock(id)
      unlocking_result = db[:wallets].update_one(
        { _id: id },
        '$set' => {
           locked_at:    nil,
           locked_until: nil
         }
      )
      fail "Update failed on releasing lock" unless unlocking_result.ok?
    end
  end

  class Runner
    def initialize(*)
    end

    def to_s
      "#{self.class.to_s}"
    end

    def get_db_connection
      $mongo ||= Mongo::Client.new(
        'mongodb://127.0.0.1:27017/locker-raw-test',
        connect_timeout: 300,
        wait_queue_timeout: 300,
        max_pool_size: 1000,
      )
    end

    def create_transaction(wallet_id, amount)
      Raw.new(self).create_transaction(wallet_id, amount)
    end

    def init(*)
      db = get_db_connection
      db[:wallets].insert_one({balance: 0, counter: 0, data: []}).inserted_id
    end

    def teardown
      db = get_db_connection
      db[:transactions].delete_many({})
      db[:wallets].delete_many({})
    end
  end
end
