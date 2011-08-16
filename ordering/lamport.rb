require 'rubygems'
require 'bud'

#lamportmsg is a [clockval, payload] pair
module LamportInterface
  state do
    interface input, :to_stamp, [] => [:msg]
    interface output, :get_stamped, [:msg] => [:lamportmsg]
    interface input, :retrieve_msg, [:lamportmsg] => []
    interface output, :msg_return, [:lamportmsg] => [:msg]
  end
end

module LamportClockManager
  include LamportInterface

  state do
    table :localclock, [] => [:clock]

    table :action_buf, [] => [:actiontype, :msg, :queuetime]

    scratch :next_stamp, to_stamp.schema
    scratch :next_retrieve, retrieve_msg.schema
  end

  bootstrap do
    localclock <= [[0]]
  end

  bloom do

    action_buf <= to_stamp { |s| ["S", s.msg, @budtime] }
    action_buf <= retrieve_msg { |r| ["R", r.lamportmsg, @budtime] }

    #todo: multiple values may be returned!
    temp :nextaction <= action_buf.argmin([action_buf.actiontype, action_buf.msg,
                                            action_buf.queuetime],
                                   action_buf.queuetime)

    get_stamped <= (localclock * nextaction).pairs do |c, m|
      if m.actiontype == "S":
        localclock <- localclock
        localclock <+ localclock { |cv| [cv.clock + 1] }
        action_buf <- nextaction

        [m.msg, [[c.clock, m.msg]]]
      end
    end

    msg_return <= nextaction do |r|
      if r.actiontype == "R":
        localclock <- localclock
        localclock <+ localclock { |cv| [[cv.clock, r.msg[0]].max()+1] }

        action_buf <- nextaction

        [r.msg, r.msg[1]]
      end
    end
  end
end


