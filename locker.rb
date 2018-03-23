require 'bundler/setup'
Bundler.require(:default)
Mongoid.load!('mongoid.yml', :development)

require 'securerandom'

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

  field :balance, type: Integer, default: 0
  field :data, type: Hash, default: {}

  def refresh_balance(transaction)
    self.balance += transaction.amount
    save!
  end

  def record_transaction(amount)
    transaction = nil
    with_lock(retries: 400, retry_sleep: 0.5, reload: true) do
      STDERR.puts "BEGIN #{Thread.current[:id]} lock: #{$lock_counter} <<< old balance=#{balance}"
      transaction = self.transactions.create!(amount: amount)
      STDERR.puts "transaction created #{Thread.current[:id]} lock: #{$lock_counter}"
      refresh_balance(transaction)
      STDERR.puts "balance refreshed #{Thread.current[:id]} lock: #{$lock_counter} >>> new balance=#{balance}"
      STDERR.puts "END #{Thread.current[:id]} lock: #{$lock_counter}"
    end

    100.times do |i|
      self.data["a_#{Time.now.to_i}_#{i}"] = transaction.id
    end
    self.save!
  end
end

def check(actual, expected, name)
  if expected == actual
    puts "#{name} is OK".green
  else
    puts "#{name} should be #{expected} but it is #{actual}.".red
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
        Wallet.find(wallet_id).record_transaction(10)
      rescue StandardError => e
        STDERR.puts e.to_s.red
      end
    end
  end
  threads.each(&:join)
  STDERR.puts "============================ END THREADING =============================="
end

def run_test
  wallet = Wallet.create
  number_of_transactions = 200

  concurrent_add_trasactions(wallet.id, number_of_transactions)
  wallet.reload

  check(wallet.transactions.count, number_of_transactions, "recorded transactions count")
  check(wallet.balance, number_of_transactions*10, "balance")
ensure
  Mongoid.purge!
end

run_test
