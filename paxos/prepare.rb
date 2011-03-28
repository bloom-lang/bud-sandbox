require 'rubygems'
require 'bud'

require 'voting/voting'

module PaxosPrepare 
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

  bloom :prep1 do
    # bootstrap these.
    #local_aru << [@myloc, 0] if global_history.empty?
    #last_installed << [0] if global_history.empty?

    prepare <= (leader_change * local_aru).pairs do |c, a|
      if c.leader == c.host
        print "prepare!\n" or [c.view, a.aru]
      end
    end

    begin_vote <+ prepare.map{|p| print "put in ballot : " + p.inspect + "\n" or [p.view, p.aru]}
  end

  bloom :establish_quorum do
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
    scratch :datalist_agg, [:view, :contents]
    table :datalist_length, [:aru, :len]
    table :global_history, [:host, :seqno] => [:requestor, :update]
    table :last_installed, [] => [:view]
    table :accept, [:view, :seq, :update]
  end

  bootstrap do
    last_installed << [0] if global_history.empty?
  end

  bloom :build_reply do
    stdio <~ ballot.map{|b| ["got ballot: #{b.inspect}"] }
    datalist <= (ballot * last_installed).pairs do |d, l|
      if d.ident > l.view
        print "AROO\n" or [d.ident, d.content, -1, "none", "bottom"]
      else 
        print "ACHOO " + d.inspect + ":: " + l.inspect + " vs. " +d.ident.to_s + "\n"
      end
    end

    datalist <= (datalist * global_history).pairs do |d, g|
      if g.seqno > d.aru_requested and d.dltype == "bottom"
        print "oh yeah\n" or [d.view, d.aru_requested, g.seqno, g.update, "ordered"]
      else
        print "oh dear.  !" + g.seqno.to_s + " > " + d.aru_requested.to_s + "\n"
      end 
    end

    datalist <= (datalist * accept).pairs do |d, a|
      if a.seq >= d.aru_requested and d.dltype == "bottom"
        [d.view, d.aru_requested, a.seq, a.update, "proposed"]
      else
        print "oh dear. !" + a.seq.to_s + " >= " + d.aru_requested.to_s + "\n"
      end
    end

    datalist_agg <= datalist.group([datalist.view], accum([datalist.aru_requested, datalist.seq, datalist.update, datalist.dltype]))
    #datalist_length <= datalist.group([datalist.aru_requested], count())

    stdio <~ datalist_agg.map{|d| ["DLA: #{d.inspect}"] } 
  end

  bloom :decide do
    temp :dj <= (datalist * datalist_length)
    #cast_vote <+ dj.map do |d, l|
    #  print "SEND " +d.view.to_s + ": " + d.inspect + "\n" or [d.view, [d.view, d.aru_requested, d.seq, d.update, d.dltype, l.len]]
    #end
    cast_vote <= datalist_agg.map{|d| [d.view, d.contents] } 
  
    #datalist <- dj.map{|d, l| d}
  end 
end

