require './test_common'
require 'statemachine/statemachine'

class SM
  include Bud
  import StateMachine => :mshn

  state do
    table :current_log, [:event_name]
  end

  bloom do
    current_log <= mshn.current
  end
end

class TestSM < MiniTest::Unit::TestCase
  def simple_cb(bud_i, tbl_name)
    q = Queue.new
    cb = bud_i.register_callback(tbl_name) do
      q.push(true)
    end
    [q, cb]
  end

  def block_for_cb(bud_i, q, cb, unregister=true)
    q.pop
    bud_i.unregister_callback(cb) if unregister
  end
  
  def test_state_machine
    rd = SM.new
    rd.run_bg

    rd.sync_do {
      rd.mshn.states <+ [['start', false], ['moving', false], ['end', true]]
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
    rd.sync_do { assert_equal(['end'], rd.mshn.current.first) }
  end
end
