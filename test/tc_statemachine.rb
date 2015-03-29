require './test_common'
require 'statemachine/statemachine'

class SM
  include Bud
  import StateMachine => :mshn

  state do
    table :current_log, [:name]
  end

  bloom do
    current_log <= mshn.current
  end
end

class TestSM < MiniTest::Unit::TestCase
 
  def test_state_machine
    rd = SM.new
    rd.run_bg

    rd.sync_do {
      rd.mshn.states <+ [['start'], ['moving'], ['end', true]]
      rd.mshn.xitions <+ [['start', 'moving', 'down'], ['moving', 'moving', 'move'], ['moving', 'end', 'up']]
    }

    rd.sync_do {
      rd.mshn.event <+ [['reset']]
    }
    rd.sync_do {
      rd.mshn.event <+ [['down']]
    }
    
    rd.sync_do
    rd.sync_do { assert_equal(['moving'], rd.mshn.current.first) }
    rd.sync_do {
      rd.mshn.event <+ [['move']]
    }
    rd.sync_do { assert_equal(['moving'], rd.mshn.current.first) }
    rd.sync_do {
      rd.mshn.event <+ [['move']]
    }
    rd.sync_do { assert_equal(['moving'], rd.mshn.current.first) }
    rd.sync_do {
      rd.mshn.event <+ [['move']]
    }
    rd.sync_do { assert_equal(['moving'], rd.mshn.current.first) }
    rd.sync_do {
      rd.mshn.event <+ [['up']]
    }
    rd.sync_do
    rd.sync_do { 
      assert_equal(['end'], rd.mshn.current.first)
      assert_equal([true], rd.mshn.result.first)
    }
  end
end
