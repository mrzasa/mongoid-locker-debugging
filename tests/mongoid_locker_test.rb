require 'minitest/autorun'
require_relative 'test'
require_relative '../locker.rb'

class MongoidLockerTest < MiniTest::Test
  include Test

  def create_runner
    LockerRunner.new('mongoid')
  end
end
