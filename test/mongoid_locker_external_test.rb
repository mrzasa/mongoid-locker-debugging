require 'minitest/autorun'
require_relative 'test'
require_relative '../runners/mongoid_locks.rb'

class MongoidLockerExternalTest < MiniTest::Test
  include Test

  def setup
    @thread_count = 500
    @process_count = 4
    super
  end

  def create_runner
    Runners::MongoidLocks::Runner.new(Runners::MongoidLocks::ExternalLockWallet)
  end
end
