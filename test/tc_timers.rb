require './test_common'
require 'timers/progress_timer'

class TT
  include Bud
  include ProgressTimer
end

class TestTimers < Test::Unit::TestCase
  def test_simple_timers
    tt = TT.new
    tt.run_bg
    tt.sync_do {
      tt.set_alarm <+ [['foo', 1]]
    }
    tt.delta(:alarm)
    tt.stop
  end
end
