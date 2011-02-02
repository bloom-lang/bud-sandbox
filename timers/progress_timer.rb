require 'rubygems'
require 'bud'
require 'time'

# there are different ways to do this.  this one only sends one "alarm", then GCs.

module ProgressTimerProto
  def state
    interface :input, :set_alarm, ['name', 'timeout']
    interface :input, :del_alarm, ['name']
    interface :output, :alarm, ['name', 'timeout']
  end
end

module ProgressTimer
  include ProgressTimerProto
  include Anise
  annotator :declare

  def state
    super
    table :timer_state, ['name'], ['start_tm', 'timeout']
    periodic :timer, 0.2
  end

  declare 
  def timer_logic
    timer_state <= join([set_alarm, timer]).map{ |s, t| [s.name, Time.parse(t.time).to_f, s.timeout] }
      
    alarm <= join([timer_state, timer]).map do |s, t|
      if Time.parse(t.time).to_f - s.start_tm > s.timeout
        [s.name, s.timeout]
      end
    end
    
    timer_state <- join([timer_state, alarm], [timer_state.name, alarm.name]).map{ |s, a| s }
    timer_state <- join([timer_state, del_alarm], [timer_state.name, del_alarm.name]).map{ |s, a| s }

  end
end

