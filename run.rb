require_relative 'locker.rb'
require_relative 'raw.rb'

def check(actual, expected, name)
  if expected == actual
    puts "#{name} is OK".green
    true
  else
    puts "#{name} should be #{expected} but it is #{actual}.".red
    false
  end
end

def concurrent_add_trasactions(runner, wallet_id, thread_count, process_count)
  if process_count <= 1
    threaded_add_trasactions(runner, wallet_id, thread_count)
  else
    process_count.times do |i|

      Process.fork { sleep(i % 2); threaded_add_trasactions(runner, wallet_id, thread_count) }
    end
    STDERR.puts "===================== PROCESS WAIT".purple
    Process.waitall
  end
end

def threaded_add_trasactions(runner, wallet_id, thread_count)
  threads = []

  STDERR.puts "========================= START THREADING ============================="
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
  STDERR.puts "============================ END THREADING =============================="
end

def run_test(runner, thread_count, process_count, params={})
  wallet_id = runner.init(params)
  db = runner.get_db_connection
  STDERR.puts ["created", db[:wallets].find(_id: wallet_id).first].to_s.yellow

  concurrent_add_trasactions(runner, wallet_id, thread_count, process_count)

  wallet_data = db[:wallets].find(_id: wallet_id).first
  balance = wallet_data[:balance]
  counter = wallet_data[:counter]
  transactions_count = db[:transactions].find(wallet_id: wallet_id).count
  check(transactions_count, thread_count * process_count, "recorded transactions count")
  check(balance, thread_count * process_count *10, "balance")
  check(counter, thread_count * process_count, "counter")
ensure
  runner.teardown
end

if __FILE__ == $0
  Mongo::Logger.logger = Logger.new(STDERR)
  Mongo::Logger.logger.level = Logger::DEBUG

  thread_count = (ARGV[0] || 10).to_i
  process_count = (ARGV[1] || 1).to_i
  locking_method = ARGV[2]
  mode = ARGV[3] || 'raw'

  runner = "#{locking_method.capitalize}Runner".constantize.new(mode)

  run_test(runner, thread_count, process_count, locking_method: mode)
end
