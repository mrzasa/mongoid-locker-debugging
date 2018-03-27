require 'bundler/setup'
Bundler.require(:default)
Mongoid.load!('mongoid.yml', :development)

require 'securerandom'

class Lock
  include Mongoid::Document

  CannotObtainLockError = Class.new(StandardError)

  store_in collection: 'locks'

  field :_id, type: String, default: ->{ SecureRandom.uuid }, overwrite: true
  field :ts, type: Time

  def self.with_lock(stream)
    begin
      lock = self.create(_id: stream, ts: Time.now.utc)
    rescue ::Mongo::Error::OperationFailure
      raise CannotObtainLockError
    end
    yield if block_given?
  ensure
    lock.delete if lock.present?
  end

  index({ ts: 1 }, { expire_after_seconds: 30 })
end

class ExternalLock
  include Mongoid::Document
  include Mongoid::Timestamps
  include Mongoid::Locker

  belongs_to :wallet
end

class Transaction
  include Mongoid::Document
  include Mongoid::Timestamps

  belongs_to :wallet

  field :amount, type: Integer
end

class Wallet
  include Mongoid::Document
  include Mongoid::Timestamps
  include Mongoid::Locker

  has_many :transactions
  has_one :external_lock, autobuild: true

  field :balance, type: Integer, default: 0
  field :data, type: Hash, default: {}
  field :rating, type: Float, default: 0
  field :counter, type: Integer, default: 0

  def with_expirable_lock
    retry_limit = 2000
    retry_count = 0
    sleep = 0.5
    begin
      Lock.with_lock(id) do
        yield
      end
    rescue Lock::CannotObtainLockError
      retry_count += 1
      sleep(sleep)
      if retry_count < retry_limit+1
        retry
      else
        STDERR.puts '--- CANNOT OBTAIN LOCK'.red
        raise
      end
    end
  end

  def with_mongoid_lock
    with_lock(retries: 1000, retry_sleep: 0.5, reload: false) do
      yield
    end
  end

  def with_external_lock
    external_lock.with_lock(retries: 400, retry_sleep: 0.5, reload: false) do
      yield
    end
  end

  def self.lock_method=(lock_method)
    @@lock_method = lock_method
  end
  def self.lock_method
    @@lock_method
  end

  def with_current_lock
    method(self.class.lock_method).call do
      yield
    end
  end

  def refresh_balance(transaction)
    self.balance += transaction.amount
    save!
  end

  def record_transaction(amount)
    transaction = nil
    with_current_lock do
      reload
      $lock_counter = $lock_counter + 1
      STDERR.puts "BEGIN #{Thread.current[:id]} lock: #{$lock_counter} <<< old balance=#{balance}".green
      transaction = self.transactions.create!(amount: amount)
      STDERR.puts "transaction created #{Thread.current[:id]} lock: #{$lock_counter}".green
      refresh_balance(transaction)
      STDERR.puts "balance refreshed #{Thread.current[:id]} lock: #{$lock_counter} >>> new balance=#{balance}".green
      STDERR.puts "END #{Thread.current[:id]} lock: #{$lock_counter}".green
    end
    store_data(transaction.id)
    # update_rating
    # increment_counter
    # bare_driver_inrement_counter
  end

  def raw_refresh_balance(amount)
    self.class.collection.update_one({_id: self.id}, {balance: self.balance + amount})
  end

  def raw_record_transaction(amount)
    with_current_lock do
      reload
      $lock_counter = $lock_counter + 1
      STDERR.puts "BEGIN #{Thread.current[:id]} lock: #{$lock_counter} <<< old balance=#{balance}".green
      Transaction.collection.insert_one(wallet_id: self.id, amount: amount)
      STDERR.puts "transaction created #{Thread.current[:id]} lock: #{$lock_counter}".green
      raw_refresh_balance(amount)
      STDERR.puts "balance refreshed #{Thread.current[:id]} lock: #{$lock_counter} >>> new balance=#{balance}".green
      STDERR.puts "END #{Thread.current[:id]} lock: #{$lock_counter}".green
    end
    store_data(rand(120))
  end

  def simple_record_transaction(amonunt)

  end

  # failing for 50
  def store_data(transaction_id)
    self.data["a_#{Time.now.to_i}_#{transaction_id}"] = transaction_id
    self.save!
  end

  #failing for 500
  def update_rating
    set(rating: rand(1000))
  end

  # failing for 500
  def increment_counter
    inc(counter: 1)
  end

  # failing for 1000, ok for 500
  def bare_driver_inrement_counter
    self.class.collection.update_one({_id: id}, {"$inc" => {counter: 1}})
  end
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

class Raw
  def db
    @db ||= Mongoid::Clients.default
  end

  def create_wallet
    db[:wallets].insert_one({balance: 0, data: []}).inserted_id
  end

  def add_transaction(wallet_id, amount)
    acquire_lock(wallet_id)
    $lock_counter = $lock_counter + 1
    puts ["BEGIN", $lock_counter].to_s.blue

    wallet_id = BSON::ObjectId(wallet_id)
    balance = db[:wallets].find(_id: wallet_id).first[:balance]
    db[:transactions].insert_one(wallet_id: wallet_id, amount: amount)
    db[:wallets].update_one({_id: wallet_id}, {balance: balance + amount})

    puts ["END", $lock_counter].to_s.blue
  ensure
    release_lock(wallet_id)
  end

  def create_transaction(wallet_id, amount)
    add_transaction(wallet_id, amount)
    increment_counter(wallet_id)
  end

  def increment_counter(wallet_id)
    db[:wallets].update_one({_id: wallet_id}, {"$inc" => {counter: 1}})
  end

  def acquire_lock(id)
    time = Time.now
    expiration = time + 5
    retry_limit = 2000
    retry_count = 0
    sleep_time = 0.5

    loop do
      locking_result = db[:wallets].update_one(
        {
          :_id => id,
          '$or' => [
            # not locked
            { mongoid_locker_locked_until: nil },
            # expired
            { mongoid_locker_locked_until: { '$lte' => time } }
          ]
        },

        '$set' => {
          mongoid_locker_locked_at:    time,
          mongoid_locker_locked_until: expiration
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
         mongoid_locker_locked_at:    nil,
         mongoid_locker_locked_until: nil,
       }
    )
    fail "Update failed on releasing lock" unless unlocking_result.ok?
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
        w = Wallet.find(wallet_id)
        w.raw_record_transaction(10)
        #check(w.balance, w.transactions.size * 10, "thread #{i}")
      rescue StandardError => e
        STDERR.puts e.to_s.red
        end
    end
  end
  threads.each(&:join)
  STDERR.puts "============================ END THREADING =============================="
end

def raw_concurrent_add_trasactions(wallet_id, number_of_transactions)
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

def raw_run_test(number_of_transactions)
  wallet_id = db[:wallets].insert_one({balance: 0, data: []}).inserted_id

  raw_concurrent_add_trasactions(wallet_id, number_of_transactions)

  balance = db[:wallets].find(_id: wallet_id).first[:balance]
  transactions_count = db[:transactions].find(wallet_id: wallet_id).count
  check(transactions_count, number_of_transactions, "recorded transactions count")
  check(balance, number_of_transactions*10, "balance")
  check(wallet.counter, number_of_transactions, "counter")
ensure

end

def run_test(number_of_transactions, lock_method)
  puts [number_of_transactions, lock_method].to_s
  Wallet.lock_method = lock_method
  wallet = Wallet.create!
  wallet.external_lock.save!

  raw_concurrent_add_trasactions(wallet.id, number_of_transactions)
  wallet.reload

  check(wallet.transactions.count, number_of_transactions, "recorded transactions count")
  check(wallet.balance, number_of_transactions*10, "balance")
  # check(wallet.counter, number_of_transactions, "counter")
ensure
  Mongoid.purge!
end

def reload
  load __FILE__
end

if __FILE__ == $0
  Mongo::Logger.logger = Logger.new(STDERR)
  Mongo::Logger.logger.level = Logger::DEBUG

  count = (ARGV[1] || 10).to_i
  if ARGV[0] == 'all'
    [:with_mongoid_lock, :with_expirable_lock].each do |lock_method|
      raw_run_test(count, lock_method)
    end
  else
    lock_method = ARGV[0] || :with_mongoid_lock
    run_test(count, lock_method)
  end
else
  Mongo::Logger.logger.level = Logger::DEBUG
end
