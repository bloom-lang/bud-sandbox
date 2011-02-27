require 'rubygems'
require 'test/unit'
require 'bud'
require 'timers/progress_timer'

class TT
  include Bud
  include ProgressTimer

  declare
  def lcl_process
    stdio <~ alarm.map{|a| [ "ALRM: " + a.inspect ] }
  end
end

class TestBEDelivery < Test::Unit::TestCase

  def test_besteffort_delivery
    tt = TT.new
    tt.run_bg
    tt.set_alarm <+ [['foo', 3]]
    sleep 5
    tt.stop_bg
  end
end
