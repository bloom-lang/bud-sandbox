require 'rubygems'
require 'bud'
require 'delivery/delivery'

module OrderedDelivery
    include DeliveryProtocol
    
    state do
    table :seq, [:val]
    channel :pipe_chan, [@dst, :src, :ident, :seq] => [:payload]
    table :seq_in [:dst, :src, :ident, :seq] => [:payload]
    table :to_be_del, [:dst, :src, :ident, :seq] => [:payload]
    table :last_del, [:value] 
end

bootstrap do
    seq <= [[0]]
    last_del <= [[0]]
end

bloom :order do
    #Check this line...  Goal is to set seq to index.
    seq_in <= pipe_in.sort.each_with_index.map {|s, d, i, p, seq| [s, d, i, seq, p]}
    seq_in <= (seq_in * seq).pairs do |p, s|
        [p.dst, p.src, p.ident, s.val + p.seq, p.payload]
    end 
    seq <+- seq_in.argmax([seq_in.seq], seq_in.seq)
end
end

bloom :snd do
    pipe_chan <~ seq_in
end

bloom :rcv do
    to_be_del <=  pipe_chan.argmin([pipe_chan.seq], pipe_chan.seq)
end

bloom :done do
    pipe_sent <= to_be_del {|p| [p.src, p.dst, p.ident, p.payload]}
    last_delivered <= to_be_del {|t| [t.seq]} 
end
end