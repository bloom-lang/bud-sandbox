require 'rubygems'
require 'bud'

require 'voting/voting'
#require 'lib/nonce'
require 'ordering/nonce'

module LeaderElection
  include MajorityVotingMaster
  include VotingAgent
  include SimpleNonce

  include Anise
  annotator :declare

  def initialize(opts, id)
    my_opts = opts.clone
    my_opts[:dump] = true
    super my_opts
    @id = id
  end

  def state
    super
    table :current_state, [], ['status', 'leader', 'vid']
    #table :current_state, ['status', 'leader', 'vid']
    scratch :will_ballot, ['nonce', 'vid', 'time']
    scratch :latest_ballot, ['time']

    periodic :timer, 3
    scratch :will_vote, ['message', 'leader', 'vid']
    scratch :found_leader, ['ballot', 'leader', 'vid']
  end

  declare
  def decide
    will_vote <= join([ballot, current_state]).map do |b, c|
      if c.status == "election" and not b.content.fetch(2).nil? and b.content.fetch(2) >= c.vid
        puts "will vote " + b.inspect or [b.content, b.content.fetch(1), b.content.fetch(2)]
      else
        puts "no vote? " + b.inspect + "," + c.inspect or [b.content, c.leader, c.vid]
      end
    end

    cast_vote <+ will_vote.map{|w| puts "casting vote for " + w.inspect or [w.message.fetch(0), [w.leader, w.vid]]}
    current_state <+ will_vote.map{|w| ['election', w.leader, w.vid]}
    current_state <- join([will_vote, current_state]).map{|w, c| c}
  end

  declare
  def le_two
    nj = join [timer, current_state, nonce]
    will_ballot <= nj.map do |t, s, n|
      if s.status == "election"
        puts @budtime.to_s + " will ballot " + n.ident.to_s or [n.ident, s.vid, Time.new.to_i]
      end
    end

    begin_vote <+ will_ballot.map{|w|  puts "begin vote " + w.inspect or [w.nonce, [w.nonce, @ip_port, w.vid]] }

    #found_leader <+ join([current_state, vote_status]).map do |c, s|
    found_leader <= join([current_state, victor]).map do |c, s|
      #print "found leader? #{v.cnt} for #{v.vote[0]}\n"
      if c.status == "election"
        puts "found leader? " + s.inspect or [s.ident, s.content.fetch(1), s.response.fetch(1)]
      end
    end

    current_state <+ found_leader.map do |f|
      if f.leader == @ip_port
        puts "setting to leader " + f.inspect or ['leader', @ip_port, f.vid]
      else
        puts "setting to follower " + f.inspect  or ['follower', f.leader, f.vid]
      end
    end

    current_state <- join([current_state, found_leader]).map{|c, f| c}
    #status <= found_leader.map{|f| [f.ballot, f.leader] }

    stdio <~ victor.map{|v| ["VIC: " + v.inspect] }
  end
end
