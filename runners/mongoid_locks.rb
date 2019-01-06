require 'bundler/setup'
Bundler.require(:default)
require 'securerandom'

Mongoid.load!('mongoid.yml', :development)

module Runners
  module MongoidLocks
    class Transaction
      include Mongoid::Document
      include Mongoid::Timestamps

      store_in collection: :transactions

      belongs_to :wallet

      field :amount, type: Integer
    end

    class Wallet
      include Mongoid::Document
      include Mongoid::Timestamps

      store_in collection: :wallets

      has_many :transactions

      field :balance, type: Integer, default: 0
      field :data, type: Hash, default: {}
      field :rating, type: Float, default: 0
      field :counter, type: Integer, default: 0
      field :completed_at, type: Time


      def init
        # for creating necessary dependencies in Runner
      end

      # Implemented in subclasses to test various types of locks.
      def with_current_lock
        fail NotImplementedError
      end

      def refresh_balance(transaction)
        self.balance += transaction.amount
        save!
      end

      def record_transaction(amount)
        transaction = nil
        with_current_lock do
          reload
          transaction = self.transactions.create!(amount: amount)
          refresh_balance(transaction)
        end
        # various actions can be chosen here, each one has its own
        # failure threshold (number of threads) described as action comment
        store_data(transaction.id)
        store_data(transaction.id)
        store_data(transaction.id)
        increment_counter
        #bare_driver_inrement_counter
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

      # failing for 1000, passing for 500
      def bare_driver_increment_counter
        self.class.collection.update_one({_id: id}, {"$inc" => {counter: 1}})
      end
    end

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

      store_in collection: :external_locks

      field :locked_at, type: Time
      field :locked_until, type: Time

      belongs_to :external_lock_wallet
    end

    # Lock implemented with Mongoid::Locker in an associated model
    class ExternalLockWallet < Wallet
      has_one :external_lock, autobuild: true

      def init
        external_lock.save!
      end

      def with_current_lock
        external_lock.with_lock(retries: 400, retry_sleep: 0.5, reload: false) do
          yield
        end
      end
    end

    # Lock implemented with Mongoid::Locker inside Walled model
    class InternalLockWallet < Wallet
      include Mongoid::Locker

      field :locked_at, type: Time
      field :locked_until, type: Time

      def with_current_lock
        with_lock(retries: 1000, retry_sleep: 0.5, reload: false) do
          yield
        end
      end
    end

    # Lock implemented with Lock class basing on expirable unique index, not on
    # comparing timestamps.
    class ExpirableLockWallet < Wallet
      def with_current_lock
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
            fail "cannot obtain lock"
          end
        end
      end
    end

    class Runner
      attr_reader :wallet_class

      def initialize(wallet_class)
        @wallet_class = wallet_class
      end

      def to_s
        "#{self.class.to_s}: wallet_class=#{wallet_class}"
      end

      def init(*)
        wallet = wallet_class.create!
        wallet.init
        wallet.id
      end

      def get_db_connection
        @db ||= Mongoid::Clients.default
      end

      def create_transaction(wallet_id, amount)
        w = wallet_class.find(wallet_id)
        w.record_transaction(amount)
      end

      def teardown
        Mongoid.purge!
      end
    end
  end
end
