require 'rubygems'
require 'bud'
require 'membership/membership'

module VoteMasterProto
  state do
    interface input, :begin_vote, [:ident, :content]
    interface output, :victor, [:ident, :content, :response]
  end
end

module VoteAgentProto
  state do
    interface input, :cast_vote, [:ident, :response]
  end
end

module VoteInterface
  # channels used by both ends of the voting protocol
  # paa: TODO: figure out the right way to mix in state
  state do
    channel :ballot, [:@peer, :master, :ident] => [:content]
    channel :vote, [:@master, :peer, :ident] => [:response]
    channel :tickler, [:@master] unless defined? tickler
  end
end

module VotingMaster
  include VoteInterface
  include VoteMasterProto
  include StaticMembership

  state do
    table :vote_status, [:ident, :content, :response]
    table :votes_rcvd, [:ident, :response, :peer]
    scratch :member_cnt, [:cnt]
    scratch :vote_cnt, [:ident, :response, :cnt]
  end

  bloom :initiation do
    # when stimulated by begin_vote, send ballots
    # to members, set status to 'in flight'
    temp :j <= (begin_vote * member)
    ballot <~ j.pairs do |b,m|
      [m.host, ip_port, b.ident, b.content]
    end
    vote_status <+ begin_vote do |b|
      [b.ident, b.content, 'in flight']
    end
    member_cnt <= member.group(nil, count)
  end

  bloom :counting do
    # accumulate votes into votes_rcvd table,
    # calculate current counts
    #stdio <~ vote { |v| ["GOT VOTE: " + v.inspect] }
    votes_rcvd <= vote { |v| [v.ident, v.response, v.peer] }
    vote_cnt <= votes_rcvd.group(
      [votes_rcvd.ident, votes_rcvd.response],
      count(votes_rcvd.peer))
  end

  bloom :summary do
    # this stub changes vote_status only on a
    # complete and unanimous vote.
    # a subclass will likely override this
    temp :sj <= (vote_status * member_cnt * vote_cnt).combos(vote_status.ident => vote_cnt.ident)
    victor <= sj do |s,m,v|
      if s.response == 'in flight' and m.cnt == v.cnt
        [v.ident, s.content, v.response]
      end
    end
    vote_status <+ victor
    vote_status <- victor do |v|
      [v.ident, v.content, 'in flight']
    end
  end
end


module VotingAgent
  include VoteInterface
  include VoteAgentProto

  state do
    table :waiting_ballots, [:ident, :content, :master]
  end

  # default for decide: always cast vote 'yes'.  expect subclasses to override
  bloom :decide do
    cast_vote <= ballot { |b| [b.ident, 'yes'] }
  end

  bloom :casting do
    # cache incoming ballots for subsequent decisions (may be delayed)
    waiting_ballots <= ballot {|b| [b.ident, b.content, b.master] }
    #stdio <~ ballot {|b| [ip_port + " PUT ballot " + b.inspect] }
    # whenever we cast a vote on a waiting ballot, send the vote
    vote <~ (cast_vote * waiting_ballots).pairs(:ident => :ident) do |v, c|
      [c.master, ip_port, v.ident, v.response]
    end
  end
end


module MajorityVotingMaster
  include VotingMaster

  bloom :summary do
    victor <= (vote_status * member_cnt * vote_cnt).combos(vote_status.ident => vote_cnt.ident) do |s, m, v|
      if s.response == "in flight" and v.cnt > m.cnt / 2
        [v.ident, s.content, v.response]
      end
    end
    vote_status <+ victor
    vote_status <- victor {|v| [v.ident, v.content, 'in flight'] }
    #localtick <~ victor
  end
end
