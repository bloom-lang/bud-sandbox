require 'rubygems'
require 'bud'
require 'time'

# there are different ways to do this.  this one only sends one "alarm", then GCs.

module ProgressTimerProto
  state do
    interface :input, :set_alarm, [:name, :time_out]
    interface :input, :del_alarm, [:name]
    interface :output, :alarm, [:name, :time_out]
  end
end

module ProgressTimer
  include ProgressTimerProto

  state do
    table :timer_state, [:name] => [:start_tm, :time_out]
    table :alrm_buf, set_alarm.schema
    periodic :timer, 0.2
  end

  bloom :timer_logic do
    alrm_buf <= set_alarm
    temp :cyc <= (alrm_buf * timer)
    timer_state <= cyc.map {|s, t| [s.name, Time.parse(t.val).to_f, s.time_out]}
    alrm_buf <- cyc.map{|s, t| s}

    alarm <= (timer_state * timer).map do |s, t|
      if Time.parse(t.val).to_f - s.start_tm > s.time_out
        [s.name, s.time_out]
      end
    end

    timer_state <- join([timer_state, alarm], [timer_state.name, alarm.name]).map {|s, a| s}
    timer_state <- join([timer_state, del_alarm], [timer_state.name, del_alarm.name]).map {|s, a| s}
  end
end
