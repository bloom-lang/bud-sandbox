require 'rubygems'
require 'bud'
require 'time'
require 'membership/membership'

HB_EXPIRE = 6.0

module HeartbeatProtocol
  include MembershipProtocol

  state do
    interface input, :payload, [] => [:payload]
    interface input, :return_address, [] => [:addy]
    interface output, :last_heartbeat, [:peer] => [:sender, :time, :payload]
  end
end

module HeartbeatAgent
  include HeartbeatProtocol

  state do
    channel :heartbeat, [:@dst, :src, :sender, :payload]
    table :heartbeat_buffer, [:peer, :sender, :payload]
    table :heartbeat_log, [:peer, :sender, :time, :payload]
    table :payload_buffer, [:payload]
    table :my_address, [] => [:addy]
    periodic :hb_timer, 2

    scratch :to_del, heartbeat_log.schema
    scratch :last_heartbeat_stg, last_heartbeat.schema
  end

  bloom :selfness do
    my_address <+ return_address
    my_address <- (my_address * return_address).pairs{ |m, r| puts "update my addresss" or m }
  end

  bloom :announce do
    heartbeat <~ (hb_timer * member * payload_buffer * my_address).combos do |t, m, p, r|
      unless m.host == r.addy
       [m.host, r.addy, ip_port, p.payload]
      end
    end

    heartbeat <~ (hb_timer * member * payload_buffer).combos do |t, m, p|
      if my_address.empty?
        unless m.host == ip_port
          [m.host, ip_port, ip_port, p.payload]
        end
      end
    end
  end

  bloom :buffer do
    payload_buffer <+ payload
    payload_buffer <- (payload_buffer * payload).lefts
  end

  bloom :reckon do
    heartbeat_buffer <= heartbeat.map{|h| [h.src, h.sender, h.payload] }
    heartbeat_log <= (hb_timer * heartbeat_buffer).pairs {|t, h| [h.peer, h.sender, Time.parse(t.val).to_f, h.payload] }
    heartbeat_buffer <- (hb_timer * heartbeat_buffer).pairs {|t, h| h }
  end

  bloom :current_output do
    #stdio <~ last_heartbeat.inspected
    last_heartbeat_stg <= heartbeat_log.argagg(:max, [heartbeat_log.peer], heartbeat_log.time)
    last_heartbeat <= last_heartbeat_stg.group([last_heartbeat_stg.peer, last_heartbeat_stg.sender, last_heartbeat_stg.time], choose(last_heartbeat_stg.payload))
    to_del <= (heartbeat_log * hb_timer).pairs do |log, t|
      if ((Time.parse(t.val).to_f) - log.time) > HB_EXPIRE
        log
      end
    end
    heartbeat_log <- to_del
  end
end
