require 'minitest/autorun'
require_relative 'test'
require_relative '../runners/mongoid_locks.rb'

class MongoidLockerInternalTest < MiniTest::Test
  include Test

  def setup
    super
  end

  def create_runner
    Runners::MongoidLocks::Runner.new(Runners::MongoidLocks::InternalLockWallet)
  end
end
