require 'rubygems'
require 'bud'
#require 'lib/bfs_client'

module HeartbeatProtocol
  def state
    super
    table :peers, ['peer']
    interface output, :last_heartbeat, ['peer', 'peer_time', 'time']
  end
end

module Heartbeat
  include HeartbeatProtocol
  include Anise
  annotator :declare

  def initialize(i, p, o)
    @addy = "#{i}:#{p}"
    super
  end

  def state
    super 
    channel :heartbeat, ['@peer', 'src', 'peer_time']
    table :heartbeat_buffer, ['peer', 'peer_time']
    table :heartbeat_log, ['peer', 'peer_time', 'time']
    periodic :hb_timer, 3
    scratch :highest, ['peer', 'time']
  end

  declare 
  def announce
    heartbeat <~ join([hb_timer, peers]).map do |t, p|
      unless p.peer == @addy
        puts @addy  + " at " + t.time.to_f.to_s + " SENDO " + p.inspect or [p.peer, @addy, t.time.to_f]
      end
    end
  end

  declare 
  def reckon
    heartbeat_buffer <= heartbeat.map{|h| [h.peer, h.peer_time] }
    duty_cycle = join [hb_timer, heartbeat_buffer]
    heartbeat_log <= duty_cycle.map{|t, h| [h.peer, h.peer_time, t.time.to_f] }
    heartbeat_buffer <- duty_cycle.map{|t, h| h } 

    highest <= heartbeat_log.group([heartbeat_log.peer], max(heartbeat_log.time))
  end

  declare 
  def current_output
    lj = join [heartbeat_log, highest], [heartbeat_log.time, highest.time]
    last_heartbeat <+ lj.map{|l, h| puts "with highest being "  + h.inspect or l}
    heartbeat_log <- join([heartbeat_log, highest]).map do |l, h|
      l unless h.time == l.time
    end
  end 
end
