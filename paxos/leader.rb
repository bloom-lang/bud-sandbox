require 'rubygems'
require 'bud'
require 'voting/voting'
require 'ordering/nonce'
require 'membership/membership'
require 'timers/progress_timer'
require 'time'

module LeaderElection
  include BudModule
  include MajorityVotingMaster
  include VotingAgent
  include GroupNonce
  include StaticMembership
  include ProgressTimer


  # disorderly leader election.
  # desired end result for each node: carry out a round of voting for a particular id (the highest we've seen)
  # till a round succeeds.

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

  declare 
  def decide
    proofj = join([proof, current_state])
    current_state <+ proofj.map do |p, s|
      if p.view > s.view 
        ['follower', p.src, p.view, s.timeout]
      end
    end 

    current_state <- proofj.map{ |p, s| s }

    # use multicast lib?
    

    start_le <= join([alarm, nonce]).map do |a, n|
      if a.name == "Progress"
        puts "TIMER KICK" or [@ip_port, n.ident, a.timeout]
      end
    end
    
    #packet <= ballot.map{ |b| (puts ip_port + " ballot: " +b.inspect) or (b.content.unshift(b.ident)) } 
    packet <= ballot.map{ |b| (puts ip_port + " ballot: " +b.inspect) or [b.ident, b.content.fetch(0), b.content.fetch(1), b.content.fetch(2)] } 
    #stdio <~ packet.map{|p| ["packet: " + p.inspect] } 
    pacstate = join([packet, current_state])
    start_le <= pacstate.map do |p, c|
      if p.view > c.view
        puts ip_port + " JUMP views to " + p.inspect + " from " + c.inspect or [p.host, p.view, c.timeout]
      end
    end

    cast_vote <= pacstate.map do |p, c| 
      puts ip_port + " cast vote " + p.inspect or [p.nonce, "yes"] if p.view >= c.view
    end
    #cast_vote <= join([packet, start_le]).map {|p, s| puts ip_port +  " cast vote for "+ s.inspect or [s, "yes"] }

    start_le <= join([init_le, nonce]).map do |i, n|
      puts ip_port + " INIT" +i.inspect + " " + n.inspect or [ip_port, n.ident, 0.5]
    end

    begin_vote <= join([start_le, nonce]).map do |s, n|
      puts ip_port + "@" + @budtime.to_s +  " BEGIN VOT FOR " + s.inspect + " with nonce " + n.inspect or [n.ident, s]
    end

    

    set_alarm <= start_le.map{ |s| puts ip_port + "@" + @budtime.to_s + " start_le : " + s.inspect or ['Progress', s.timeout * 2] }


    csj = join [current_state, start_le]
    current_state <- csj.map{|c, s| c } 
    current_state <+ csj.map{|c, s| ['election', s.host, s.view, s.timeout * 2] } 

    packet_in <= victor.map{ |v| puts ip_port + "@" + @budtime.to_s + " VIC " + v.inspect or v.content if v.response == "yes" }
    current_state <+ packet_in.map do |p|
      if p.host == ip_port
        ['leader', ip_port, p.view, p.timeout]
      else
        ['follower', p.host, p.view, p.timeout]
      end
    end
  
    current_state <- join([current_state, packet_in]).map{ |c, p| c }

    localtick <~ victor.map{|v| [ip_port] } 

  end
 
end
