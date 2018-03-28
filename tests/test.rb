
require_relative '../run.rb'

module Test
  def setup
    @wallet_id = runner.init({})
  end

  def teardown
    runner.teardown
  end

  def runner
    @runner ||= create_runner
  end

  def create_runner
    fail NotImplementedError
  end

  def test_transaction_creation
    db = runner.get_db_connection

    thread_count = (ENV['THREAD_COUNT'] || 10).to_i
    process_count = (ENV['PROCESS_COUNT'] || 2).to_i

    concurrent_add_trasactions(runner, @wallet_id, thread_count, process_count)

    wallet_data = db[:wallets].find(_id: @wallet_id).first
    balance = wallet_data[:balance]
    counter = wallet_data[:counter]
    transactions_count = db[:transactions].find(wallet_id: @wallet_id).count

    assert_equal(thread_count * process_count, transactions_count, "recorded transactions count")
    assert_equal(thread_count * process_count * 10, balance, "balance")
    assert_equal(thread_count * process_count, counter, "counter")
  end
end
