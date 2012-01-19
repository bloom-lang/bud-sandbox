require 'rubygems'
require 'bud'
#require 'backports'

require 'voting/voting'

module PaxosDatalist
  state do
    scratch :datalist_schm, [:view, :aru_requested, :seq, :update, :dltype]
  end
end

module PaxosPrepare 
  include MajorityVotingMaster
  include PaxosDatalist

  state do
    table :local_aru, [] => [:host, :aru]
    scratch :leader_change, [:host] => [:leader, :view]
  
    scratch :prepare, [:view, :aru]
    scratch :catchup_info, datalist_schm.schema
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

    begin_vote <+ prepare {|p| print "put in ballot : " + p.inspect + "\n" or [p.view, p.aru]}
  end

  bloom :establish_quorum do
    stdio <~ catchup_info {|c| ["CEE: #{c.inspect}"]}
    catchup_info <= victor.flat_map do |v|
      v.resp_content.flat_map do |c|
        c
      end
    end

    # apply state!
    quorum <= catchup_info do |c|
      [c.view, c.aru_requested] if c.dltype == 'bottom'
    end

    stdio <~ quorum {|q| ["QUORUM: #{q.inspect}"] }
  end
  
end

module PaxosPrepareAgent 
  include VotingAgent
  include PaxosDatalist

  state do
    #table :datalist, [:view, :aru_requested, :seq, :update, :dltype]
    table :datalist, datalist_schm.schema
    scratch :datalist_nest, [:view, :nest]
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
    stdio <~ ballot {|b| ["got ballot: #{b.inspect}"] }
    datalist <= (ballot * last_installed).pairs do |d, l|
      if d.ident > l.view
        [d.ident, d.content, -1, "none", "bottom"]
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

    datalist_nest <= datalist {|d| [d.view, d]}
    datalist_agg <= datalist_nest.group([datalist_nest.view], accum(datalist_nest.nest))
    stdio <~ datalist_agg {|d| ["DLA: #{d.inspect}"] } 
  end

  bloom :decide do
    temp :dj <= (datalist * datalist_length)
    cast_vote <= datalist_agg {|d| [d.view, "Y", d.contents] } 
    datalist <- dj {|d, l| d}
  end 
end

