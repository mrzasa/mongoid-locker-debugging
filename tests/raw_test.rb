require 'minitest/autorun'
require_relative 'test'
require_relative '../raw.rb'

class RawTest < MiniTest::Test
  include Test

  def create_runner
    RawRunner.new
  end
end
