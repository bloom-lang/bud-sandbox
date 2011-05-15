require 'voting/voting'

module TwoPCAgent
  include VotingAgent
  # 2pc is a specialization of voting:
  # * ballots describe transactions
  # * voting is Y/N.  A single N vote should cause abort.
  state do
    scratch :can_commit, [:xact, :decision]
  end

  bloom :decide do
    cast_vote <= (waiting_ballots * can_commit).pairs(:ident => :xact) {|w, c| puts @ip_port + " agent cast vote " + c.inspect or [w.ident, c.decision] }
  end
end

module TwoPCVotingMaster
  include VotingMaster

  # override the default summary s.t. a single N vote
  # makes the vote_status = ABORT
  bloom :summary do
    victor <= (vote_status * member_cnt * vote_cnt).combos(vote_status.ident => vote_cnt.ident) do |s, m, v|
      if v.response == "N"
        [v.ident, s.content, "N"]
      # huh??
      #elsif v.cnt > m.cnt / 2
      elsif v.cnt == m.cnt
        [v.ident, s.content, v.response]
      end
    end

    vote_status <+ victor {|v| v }
    vote_status <- victor {|v| [v.ident, v.content, 'in flight'] }
    #localtick <~ victor {|v| [@ip_port]}
  end
end


module TwoPCMaster
  include TwoPCVotingMaster
  # 2pc is a specialization of voting:
  # * ballots describe transactions
  # * voting is Y/N.  A single N vote should cause abort.
  state do
    table :xact, [:xid, :data] => [:status]
    scratch :request_commit, [:xid] => [:data]
  end

  bloom :boots do
    xact <= request_commit {|r| [r.xid, r.data, 'prepare'] }
    #stdio <~ request_commit {|r| ["begin that vote"]}
    begin_vote <= request_commit {|r| [r.xid, r.data] }
  end

  bloom :panic_or_rejoice do
    temp :decide <= (xact * vote_status).pairs(:xid => :ident)
    xact <+ decide do |x, s|
      [x.xid, x.data, "abort"] if s.response == "N"
    end

    xact <- decide do |x, s|
      x if s.response == "N"
    end

    #stdio <~ decide { |x, s| ["COMMITTING"] if s.response == "Y" }
    xact <+ decide { |x, s| [x.xid, x.data, "commit"] if s.response == "Y" }
    xact <- decide { |x, s| [x.xid, x.data, "prepare"] if s.response == "Y" }
  end

end

module Monotonic2PCMaster
  include VotingMaster

  def initialize(opts)
    super
    xact_order << ['prepare', 0]
    xact_order << ['commit', 1]
    xact_order << ['abort', 2]
  end

  state do
    # TODO
    table :xact_order, [:status] => [:ordinal]
    table :xact_final, [:xid, :ordinal]
    scratch :xact, [:xid, :data, :status]
    table :xact_accum, [:xid, :data, :status]
    scratch :request_commit, [:xid] => [:data]
    scratch :sj, [:xid, :data, :status, :ordinal]
  end

  bloom :boots do
    xact_accum <= request_commit {|r| [r.xid, r.data, 'prepare'] }
    begin_vote <= request_commit {|r| [r.xid, r.data] }
  end

  bloom :panic_or_rejoice do
    decide = (xact_accum*vote_status).pairs(:xid => :ident)
    xact_accum <= decide do |x, s|
      [x.xid, x.data, "abort"] if s.response == "N"
    end

    xact_accum <= decide do |x, s|
      [x.xid, x.data, "commit"] if s.response == "Y"
    end
  end

  bloom :twopc_status do
    sj <= (xact_accum*xact_order).pairs(:status => :status) do |x,o|
      [x.xid, x.data, x.status, o.ordinal]
    end
    xact_final <= sj.group([sj.xid], max(sj.ordinal))
    xact <= (sj*xact_final).pairs(:ordinal => :ordinal) do |s, x|
      [s.xid, s.data, s.status]
    end
  end
end
