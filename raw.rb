require 'securerandom'
require 'pry'
require 'awesome_print'
require 'mongo'

Mongo::Monitoring::CommandLogSubscriber::LOG_STRING_LIMIT = 2_000


class Raw
  def db
    @db ||= get_db_connection
  end

  def create_wallet
    db[:wallets].insert_one({balance: 0, data: []}).inserted_id
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

  def create_transaction(wallet_id, amount)
    STDERR.puts ["before transaction", db[:wallets].find(_id: wallet_id).first].to_s.yellow
    add_transaction(wallet_id, amount)
    increment_counter(wallet_id)
  end

  def increment_counter(wallet_id)
    data = db[:wallets].find(_id: wallet_id).first
    STDERR.puts ["before incrementing", db[:wallets].find(_id: wallet_id).first].to_s.yellow
    res = db[:wallets].update_one({_id: wallet_id}, {"$inc" => {counter: 1}})
    fail "Increment counter failed" unless res.ok?
    STDERR.puts ["incremented", db[:wallets].find(_id: wallet_id).first].to_s.yellow
  end

  def acquire_lock(id)
    time = Time.now
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

def get_db_connection
  $mongo ||= Mongo::Client.new(
    'mongodb://127.0.0.1:27017/locker-raw-test',
    connect_timeout: 100,
    wait_queue_timeout: 100,
    max_pool_size: 10000,
  )
end

def check(actual, expected, name)
  if expected == actual
    puts "#{name} is OK".green
    true
  else
    puts "#{name} should be #{expected} but it is #{actual}.".red
    false
  end
end

def concurrent_add_trasactions(wallet_id, number_of_transactions)
  threads = []

  STDERR.puts "========================= START THREADING ============================="
  $lock_counter = 0
  number_of_transactions.times do |i|
    threads << Thread.new do
      Thread.current[:id] = i
      # split threads into 2 groups
      sleep(i % 2)
      # no exceptions, or expectations in the threads or join all will fail
      begin
        Raw.new.create_transaction(wallet_id, 10)
        #check(w.balance, w.transactions.size * 10, "thread #{i}")
      #rescue StandardError => e
      #  STDERR.puts e.to_s.red
      end
    end
  end
  threads.each(&:join)
  STDERR.puts "============================ END THREADING =============================="
end

def run_test(number_of_transactions)
  db = get_db_connection
  wallet_id = db[:wallets].insert_one({balance: 0, counter: 0, data: []}).inserted_id
  STDERR.puts ["created", db[:wallets].find(_id: wallet_id).first].to_s.yellow

  concurrent_add_trasactions(wallet_id, number_of_transactions)

  wallet_data = db[:wallets].find(_id: wallet_id).first
  balance = wallet_data[:balance]
  counter = wallet_data[:counter]
  transactions_count = db[:transactions].find(wallet_id: wallet_id).count
  check(transactions_count, number_of_transactions, "recorded transactions count")
  check(balance, number_of_transactions*10, "balance")
  check(counter, number_of_transactions, "counter")
ensure
  db[:transactions].delete_many({})
  db[:wallets].delete_many({})
end

if __FILE__ == $0
  Mongo::Logger.logger = Logger.new(STDERR)
  Mongo::Logger.logger.level = Logger::DEBUG

  count = (ARGV[0] || 10).to_i
  run_test(count)
else
  Mongo::Logger.logger.level = Logger::DEBUG
end
