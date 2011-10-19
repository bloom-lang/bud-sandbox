require 'rubygems'
require 'bud'
require 'delivery/delivery'

module OrderedDelivery
  include NewDeliveryProtocol
  include DeliveryProtocol
  import ReliableDelivery => :rd

  state do
    table :counter, [:val]
    scratch :helper, [] => [:cnt]
    channel :ack, [:@src, :dst, :ident, :count]
    channel :pipe_chan, [@dst, :src, :ident, :count] => [:payload]
    table :count_in [:dst, :src, :ident, :count] => [:payload]
    table :to_be_del, [:dst, :src, :ident, :count] => [:payload]
    table :last_del, [:value]
    table :buffer, [:dst, :src, :ident, :count] => [:payload]   
  end
  
  bootstrap do
    counter <= [[0]]
    last_del <= [[0]]
  end


  bloom :order do
    count_in <= (pipe_in * counter).pairs do |p, c|
      [p.dst, p.src, p.ident, c.val, p.payload]
      end #Does this give different values if there is more than 1 message in pipe_in?
    helper <= pipe_in.group(nil, count); #check this
    counter <+- (counter*helper).pairs do |c, h|
      [c.val + h.cnt]
    end
    buffer <= count_in
    end
    

  bloom :snd do
    to_be_del <= buffer.argmin([buffer.dst, buffer,src], buffer.count)
    pipe_chan <~ to_be_del
  end

  bloom :rcv do
    pipe_out <= pipe_chan {|p| [p.src, p.dst, p.ident, p.payload]}
  end

  bloom :done do
    temp :acked <= (buffer*pipe_out).lefts(:ident => :ident)
    pipe_sent <= pipe_out
    last_delivered <= counter {|c| [c.val + 1]}
  end
end


