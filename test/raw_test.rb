require 'minitest/autorun'
require_relative 'test'
require_relative '../runners/raw.rb'

class RawTest < MiniTest::Test
  include Test

  def setup
    @thread_count = 500
    @process_count = 4
    super
  end

  def create_runner
    Runners::Raw::Runner.new
  end
end
