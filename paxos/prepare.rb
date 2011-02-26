require 'rubygems'
require 'bud'

require 'voting/voting'

module PaxosPrepare 
  include BudModule
  include MajorityVotingMaster

  state do
    table :local_aru, [] => [:host, :aru]
    scratch :leader_change, [:host] => [:leader, :view]
  
    scratch :prepare, [:view, :aru]
    table :quorum, [:view, :aru]
  end


  bootstrap do
    local_aru << [@myloc, 0] #if global_history.empty?
  end

  declare 
  def prep1
    # bootstrap these.
    #local_aru << [@myloc, 0] if global_history.empty?
    #last_installed << [0] if global_history.empty?

    prepare <= join([leader_change, local_aru]).map do |c, a|
      if c.leader == c.host
        print "prepare!\n" or [c.view, a.aru]
      end
    end

    begin_vote <+ prepare.map{|p| print "put in ballot : " + p.inspect + "\n" or [p.view, p.aru]}
  end

  declare 
  def establish_quorum
    quorum <= vote_status.map do |v|
      puts "VOTE_STATUS: #{v.inspect}"
      if v.response.class == Array 
        [ v.response.fetch(0), v.response.fetch(1) ] if v.response.fetch(4) == 'bottom'
      end
    end

    stdio <~ quorum.map{|q| ["QUORUM: #{q.inspect}"] }
  end
  
end

module PaxosPrepareAgent 
  include VotingAgent

  state do
    table :datalist, [:view, :aru_requested, :seq, :update, :dltype]
    table :datalist_length, [:aru, :len]
    table :global_history, [:host, :seqno] => [:requestor, :update]
    table :last_installed, [] => [:view]
    table :accept, [:view, :seq, :update]
  end

  bootstrap do
    last_installed << [0] if global_history.empty?
  end

  declare
  def build_reply
    stdio <~ ballot.map{|b| ["got ballot: #{b.inspect}"] }
    datalist <= join([ballot, last_installed]).map do |d, l|
      if d.ident > l.view
        print "AROO\n" or [d.ident, d.content, -1, "none", "bottom"]
      else 
        print "ACHOO " + d.inspect + ":: " + l.inspect + " vs. " +d.ident.to_s + "\n"
      end
    end

    datalist <= join([datalist, global_history]).map do |d, g|
      if g.seqno > d.aru_requested and d.dltype == "bottom"
        print "oh yeah\n" or [d.view, d.aru_requested, g.seqno, g.update, "ordered"]
      else
        print "oh dear.  !" + g.seqno.to_s + " > " + d.aru_requested.to_s + "\n"
      end 
    end

    datalist <= join([datalist, accept]).map do |d, a|
      if a.seq >= d.aru and d.dltype == "bottom"
        [d.view, d.aru_requested, a.seq, a.update, "proposed"]
      else
        print "oh dear. !" + a.seq.to_s + " >= " + d.aru.to_s + "\n"
      end
    end

    datalist_length <= datalist.group([datalist.aru_requested], count())
  end

  declare
  def decide
    dj = join([datalist, datalist_length])
    cast_vote <+ dj.map do |d, l|
      print "SEND " +d.view.to_s + ": " + d.inspect + "\n" or [d.view, [d.view, d.aru_requested, d.seq, d.update, d.dltype, l.len]]
    end
  
    datalist <- dj.map{|d, l| d}
  end 
end

