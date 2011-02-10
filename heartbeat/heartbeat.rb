require 'rubygems'
require 'bud'
require 'time'
#require 'lib/bfs_client'
require 'membership/membership'

module HeartbeatProtocol
  include MembershipProto
  def state
    super
    interface output, :last_heartbeat, ['peer', 'peer_time', 'time']
  end
end

module HeartbeatAgent
  include HeartbeatProtocol
  include Anise
  annotator :declare

  def initialize(opts)
    super
  end

  def state
    super 
    channel :heartbeat, ['@dst', 'src', 'peer_time']
    table :heartbeat_buffer, ['peer', 'peer_time']
    table :heartbeat_log, ['peer', 'peer_time', 'time']
    periodic :hb_timer, 3
    scratch :highest, ['peer','time']
  end

  declare 
  def announce
    heartbeat <~ join([hb_timer, member]).map do |t, p|
      unless p.host == ip_port
        [p.host, ip_port, Time.parse(t.time).to_f]
      end
    end
  end

  declare 
  def reckon
    heartbeat_buffer <= heartbeat.map{|h| [h.src, h.peer_time] }
    duty_cycle = join [hb_timer, heartbeat_buffer]
    heartbeat_log <= duty_cycle.map{|t, h| [h.peer, h.peer_time, Time.parse(t.time).to_f] }
    heartbeat_buffer <- duty_cycle.map{|t, h| h } 

    highest <= heartbeat_log.group([heartbeat_log.peer], max(heartbeat_log.time))
  end

  declare 
  def current_output
    lj = join [heartbeat_log, highest], [heartbeat_log.peer, highest.peer], [heartbeat_log.time, highest.time]
    last_heartbeat <+ lj.map{|l, h| l}
    #heartbeat_log <- join([heartbeat_log, highest], [heartbeat_log.peer, highest.peer]).map do |l, h|
    #  if h.time > l.time
    #    #puts "delete " + l.inspect + " b/c it's not the highest time " + h.inspect or l
    #  end
    #end
  end 
end
