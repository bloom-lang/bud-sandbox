require 'rubygems'
require 'bud'

module ChatProtocol
  state do
    channel :connect, [:@addr, :client] => [:nick]
    channel :chatter
  end

  DEFAULT_ADDR = "127.0.0.1:12345"

  def pretty_print(val)
    str = "\033[34m"+val[1].to_s + ": " + "\033[31m" + (val[3].to_s || '') + "\033[0m"
    pad = "(" + val[2].strftime("%I:%M.%S").to_s + ")"
    return str + " "*[66 - str.length,2].max + pad
  end
end



module ChatClient
  include ChatProtocol

  def initialize(nick=nil, server=DEFAULT_ADDR, opts={})
    @nick = nick
    @server = server
    super opts
  end

  bootstrap do
    connect <~ [[@server, ip_port, @nick]]
    nodelist <+ [[ip_port, @nick]]
  end

  bloom do
    chatter <~ (stdio * leader).pairs do |s, l|
      [l.addr, [ip_port, @nick, Time.new, s.line]]
    end
    stdio <~ chatter { |m| [pretty_print(m.val)] }
  end
end

module ChatServer
  include ChatProtocol

  state { table :nodelist }

  bloom do
    nodelist <= connect{|c| [c.client, c.nick]}
    chatter <~ (chatter * nodelist * leader).combos do |m, n, l| 
      if l.addr == ip_port and n.key != ip_port
        [n.key, m.val]
      end
    end
  end
end


module Members
  state do
    periodic :interval, 1
    channel :heartbeat, [:@to, :from]
    table :recently_seen, heartbeat.key_cols + [:rcv_time]
    scratch :live_nodes, [:addr]
    interface output, :leader, [:addr]
  end

  bloom do
    heartbeat <~ (interval * nodelist).rights{|n| [n.key, ip_port]}
    recently_seen <= heartbeat{|h| h.to_a + [Time.now.to_i]}
    live_nodes <= recently_seen.group([:from], max(:rcv_time)) do |n|
      [n.first] unless  (Time.now.to_i - n.last > 3)
    end
    leader <= live_nodes.group([], min(:addr))
  end

  bloom :dissem do
    connect <~ (interval * nodelist * nodelist).combos do |h, n1, n2|
      [n1.key, n2.key, n2.val]
    end
  end
end

class SingleChat
  include Bud
  include ChatClient
  include ChatServer
  include Members

  bloom :eggy do
    stdio <~ (chatter * leader).pairs do |m, l|
      if m.val.last == "LEADER"
        ["LEADER: #{l}"]
      end
    end
  end
end
