require 'rubygems'
require 'bud'
require 'time'

require 'voting/voting'
#require 'lib/nonce'
require 'ordering/nonce'


module LeaderElection
  include MajorityVotingMaster
  include VotingAgent
  include SimpleNonce
  include Anise
  annotator :declare

  def state
    super
    table :current_state, [], ['status', 'leader', 'vid', 'start_tm', 'timeout', 'ballot_id']
    #table :current_state, ['status', 'leader', 'vid']
    scratch :rcv_ballot, ['ident', 'vid', 'host']
    scratch :rcv_victor, ['ident', 'vid', 'host']
    scratch :will_ballot, ['ident', 'vid', 'host']
    scratch :latest_ballot, ['time']

    periodic :timer, 1
    scratch :will_vote, ['ident', 'leader', 'vid', 'timeout']
    scratch :rcv_vote, ['message', 'leader', 'vid', 'timeout']
    scratch :found_leader, ['ballot', 'leader', 'vid', 'timeout']
  end

  declare
  def decide
    # need to override "decide" from voting parent module
    will_ballot <= join([timer, current_state, nonce]).map do |t, s, n|
      if s.status == "election" and Time.parse(t.time).to_i - s.start_tm > s.timeout
        puts @ip_port + "@" + @budtime.to_s + " will ballot " + n.ident.to_s or [n.ident, [n.ident, s.vid, @ip_port]]
      end
    end

    begin_vote <= will_ballot.map{ |b| [b.ident, b] }
    csj = join [current_state, will_ballot]
    current_state <+ csj.map{ |s, b| ['election', b.host, b.vid, c.start_tm,

    rcv_ballot <= ballot.map{ |b| puts @ip_port + " got ballot " + b.inspect or b.content } 

    # or progress timer fires...

    will_vote <= join([rcv_ballot, current_state]).map do |b, c|
      if c.status == "election" and (not b.vid.nil?) and b.vid > c.vid or (b.host == @ip_port and b.vid == c.vid)
        puts @ip_port + "vote for other "  + b.inspect + " current " + c.inspect  or [b.ident, b.host, b.vid, c.timeout * 2]
      #else
        #stdio <~ [[ @ip_port + " doesn't vote for ballot " + b.inspect ]] 
      #  puts @ip_port + "no vote? " + b.inspect + "," + c.inspect if @ip_port != b.content.fetch(1) #or [b.content, c.leader, c.vid, c.timeout * 2]
      end
    end

    # or we hear of a higher ballot
    begin_vote <= join([will_vote, nonce]).map do |w, n| 
      puts "vot for the other guy" or [n.ident, [n.ident, w.vid, @ip_port]] 
    end
    
    cast_vote <+ will_vote.map{|w| puts @ip_port + "casting vote for " + w.inspect or [w.ident, [w.ident, w.leader, w.vid]]}
    current_state <+ will_vote.map{|w| ['election', w.leader, w.vid, Time.new.to_i, w.timeout, w.ident]}
    current_state <- join([will_vote, current_state]).map{|w, c| c}
  end

  declare
  def le_two 
    rcv_victor <= victor.map{ |v| puts "VIC: " + v.inspect or v.content }

    found_leader <= join([current_state, rcv_victor]).map do |c, v|
      if c.status == "election" 
        puts @ip_port + " found leader? " + v.inspect or [v.ident, v.host, v.vid, c.timeout]
      end
    end

    current_state <+ found_leader.map do |f|
      if f.leader == @ip_port
        puts @ip_port + " setting to leader " + f.inspect or ['leader', @ip_port, f.vid, Time.new.to_f, f.timeout]
      else
        puts @ip_port + " setting to follower " + f.inspect  or ['follower', f.leader, f.vid, Time.new.to_f, f.timeout]
      end
    end

    current_state <- join([current_state, found_leader]).map{|c, f| c}
    #stdio <~ victor.map{|v| [" VIC: " + @ip_port  + " " + v.inspect] }
  end
end
