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

    thread_count  = (ENV['THREAD_COUNT'] || @thread_count || 10).to_i
    process_count = (ENV['PROCESS_COUNT'] || @process_count || 2).to_i

    puts "== threads: #{thread_count}, processes: #{process_count}".green

    concurrent_add_trasactions(runner, @wallet_id, thread_count, process_count)

    wallet_data = db[:wallets].find(_id: @wallet_id).first
    balance = wallet_data[:balance]
    counter = wallet_data[:counter]
    transactions_count = db[:transactions].find(wallet_id: @wallet_id).count

    puts "== threads: #{thread_count}, processes: #{process_count}".green
    assert_equal(thread_count * process_count, transactions_count, "recorded transactions count")
    assert_equal(thread_count * process_count, counter, "counter")
    assert_equal(thread_count * process_count * 10, balance, "balance")
  end

  def concurrent_add_trasactions(runner, wallet_id, thread_count, process_count)
    if process_count <= 1
      threaded_add_trasactions(runner, wallet_id, thread_count)
    else
      process_count.times do |i|
        Process.fork { sleep(i % 2); threaded_add_trasactions(runner, wallet_id, thread_count) }
      end
      Process.waitall
    end
  end

  def threaded_add_trasactions(runner, wallet_id, thread_count)
    threads = []

    $lock_counter = 0
    thread_count.times do |i|
      threads << Thread.new do
        Thread.current[:id] = i
        # split threads into 2 groups
        sleep(i % 2)
        runner.create_transaction(wallet_id, 10)
      end
    end
    threads.each(&:join)
  end
end
