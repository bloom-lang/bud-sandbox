require 'rubygems'
require 'bud'
require 'time'
#require 'lib/bfs_client'
require 'membership/membership'

module HeartbeatProtocol
  include MembershipProto
  def state
    super
    interface input, :payload, [], ['payload']
    interface output, :last_heartbeat, ['peer', 'peer_time', 'time', 'payload']
  end
end

module HeartbeatAgent
  include HeartbeatProtocol
  include Anise
  annotator :declare

  def state
    super 
    channel :heartbeat, ['@dst', 'src', 'peer_time', 'payload']
    table :heartbeat_buffer, ['peer', 'peer_time', 'payload']
    table :heartbeat_log, ['peer', 'peer_time', 'time', 'payload']
    table :payload_buffer, ['payload']
    #periodic :hb_timer, 3
    periodic :hb_timer, 1
    scratch :highest, ['peer','time']
  end

  declare 
  def announce
    heartbeat <~ join([hb_timer, member, payload_buffer]).map do |t, m, p|
      unless m.host == ip_port
        puts "SEND HB #{p.payload.inspect}" or [m.host, ip_port, Time.parse(t.time).to_f, p.payload]
      end
    end
  end
  
  declare
  def buffer
    payload_buffer <+ payload
    payload_buffer <- join([payload_buffer, payload]).map{|b, p| b }
  end 

  declare 
  def reckon
    stdio <~ hb_timer.map{|t| ["TICK with #{member.length} members"] } 
    heartbeat_buffer <= heartbeat.map{|h| [h.src, h.peer_time, h.payload] }
    duty_cycle = join [hb_timer, heartbeat_buffer]
    heartbeat_log <= duty_cycle.map{|t, h| [h.peer, h.peer_time, Time.parse(t.time).to_f, h.payload] }
    heartbeat_buffer <- duty_cycle.map{|t, h| h } 
    highest <= heartbeat_log.group([heartbeat_log.peer], max(heartbeat_log.time))
  end

  declare 
  def current_output
    lj = join [heartbeat_log, highest], [heartbeat_log.peer, highest.peer], [heartbeat_log.time, highest.time]
    last_heartbeat <+ lj.map{|l, h| l}
    #heartbeat_log <- join([heartbeat_log, highest, hb_timer], [heartbeat_log.peer, highest.peer]).map do |l, h, t|
      #if h.time > l.time
      #  puts "delete " + l.inspect + " b/c it's not the highest time " + h.inspect 
    #    puts "H  is #{h.time} vs. #{l.time}" or l
      #end
    #end
  end 
end
