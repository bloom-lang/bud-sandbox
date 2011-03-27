require 'rubygems'
require 'test/unit'
require 'bud'
require 'timers/progress_timer'

class TT
  include Bud
  include ProgressTimer

end

class TestTimers < Test::Unit::TestCase

  def test_besteffort_delivery
    tt = TT.new(:dump_rewrite => true)
    tt.run_bg
    tt.set_alarm <+ [['foo', 1]]
    tt.sync_do{}
    #sleep 2
    tt.delta(:alarm)
    puts "out"
    tt.stop_bg
  end
end
