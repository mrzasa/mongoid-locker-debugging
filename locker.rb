require 'bundler/setup'
Bundler.require(:default)

Mongoid.load!('mongoid.yml', :development)

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

  index({ ts: 1 }, { expire_after_seconds: 1 })
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
  field :lock_counter, type: Integer, default: 100

  field :locked_at, type: Time
  field :locked_until, type: Time

  field :completed_at, type: Time

  def with_expirable_lock
    retry_limit = 20000
    retry_count = 0
    sleep = 0.4
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
    increment_counter
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
    bare_driver_increment_counter
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
  def bare_driver_increment_counter
    self.class.collection.update_one({_id: id}, {"$inc" => {counter: 1}})
  end

end

class LockerRunner
  def initialize(lock_method)
    @lock_method = lock_method
  end

  def to_s
    "#{self.class.to_s}: lock_method=#{@lock_method}"
  end

  require 'securerandom'

  def init(params)
    Wallet.lock_method = "with_#{params.fetch(:locking_method, @lock_method)}_lock"
    wallet = Wallet.create!
    wallet.external_lock.save!
    wallet.id
  end

  def get_db_connection
    @db ||= Mongoid::Clients.default
  end

  def create_transaction(wallet_id, amount)
    w = Wallet.find(wallet_id)
    w.record_transaction(amount)
  end

  def teardown
    Mongoid.purge!
  end
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
  # Mongo::Logger.logger.level = Logger::DEBUG
end
