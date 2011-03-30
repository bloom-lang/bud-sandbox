require 'rubygems'
require 'bud'
require 'voting/voting'
require 'ordering/nonce'
require 'membership/membership'
require 'timers/progress_timer'
require 'time'

module LeaderElection
  include MajorityVotingMaster
  include VotingAgent
  include GroupNonce
  include StaticMembership
  include ProgressTimer

  # disorderly leader election.  desired end result for each node: carry out a
  # round of voting for a particular id (the highest we've seen) till a round
  # succeeds.

  # reactions:
  # * progress timer fires.  increment view id, and start voting
  # * see a ballot for a higher view id.

  state do
    table :current_state, [] => [:state, :leader, :view, :timeout]
    scratch :packet, [:nonce, :host, :view, :timeout]
    scratch :packet_in, [:host, :view, :timeout]
    scratch :start_le, [:host, :view, :timeout]
    scratch :init_le, []

    channel :proof, [:@host, :src, :view]
  end

  bloom :decide do
    temp :proofj <= (proof * current_state)
    current_state <+ proofj do |p, s|
      if p.view > s.view 
        ['follower', p.src, p.view, s.timeout]
      end
    end 

    current_state <- proofj { |p, s| s }

    # use multicast lib?
    

    start_le <= (alarm * nonce).pairs do |a, n|
      if a.name == "Progress"
        puts "TIMER KICK" or [@ip_port, n.ident, a.timeout]
      end
    end
    
    #packet <= ballot { |b| (puts ip_port + " ballot: " +b.inspect) or (b.content.unshift(b.ident)) } 
    packet <= ballot { |b| (puts ip_port + " ballot: " +b.inspect) or [b.ident, b.content.fetch(0), b.content.fetch(1), b.content.fetch(2)] } 
    #stdio <~ packet {|p| ["packet: " + p.inspect] } 
    temp :pacstate <= (packet * current_state)
    start_le <= pacstate do |p, c|
      if p.view > c.view
        puts ip_port + " JUMP views to " + p.inspect + " from " + c.inspect or [p.host, p.view, c.timeout]
      end
    end

    cast_vote <= pacstate do |p, c| 
      puts ip_port + " cast vote " + p.inspect or [p.nonce, "yes"] if p.view >= c.view
    end
    #cast_vote <= (packet * start_le).pairs {|p, s| puts ip_port +  " cast vote for "+ s.inspect or [s, "yes"] }

    start_le <= (init_le * nonce).pairs do |i, n|
      puts ip_port + " INIT" +i.inspect + " " + n.inspect or [ip_port, n.ident, 0.5]
    end

    begin_vote <= (start_le * nonce).pairs do |s, n|
      puts ip_port + "@" + @budtime.to_s +  " BEGIN VOT FOR " + s.inspect + " with nonce " + n.inspect or [n.ident, s]
    end

    

    set_alarm <= start_le { |s| puts ip_port + "@" + @budtime.to_s + " start_le : " + s.inspect or ['Progress', s.timeout * 2] }


    temp :csj <=  (current_state* start_le)
    current_state <- csj {|c, s| c } 
    current_state <+ csj {|c, s| ['election', s.host, s.view, s.timeout * 2] } 

    packet_in <= victor { |v| puts ip_port + "@" + @budtime.to_s + " VIC " + v.inspect or v.content if v.response == "yes" }
    current_state <+ packet_in  do |p|
      if p.host == ip_port
        ['leader', ip_port, p.view, p.timeout]
      else
        ['follower', p.host, p.view, p.timeout]
      end
    end
  
    current_state <- (current_state * packet_in).pairs{ |c, p| c }

    localtick <~ victor {|v| [ip_port]}
  end
end
