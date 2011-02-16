require 'rubygems'
require 'bud'
require 'delivery/delivery'

module ReliableDelivery
  include  BestEffortDelivery

  state {
    table :pipe, [:dst, :src, :ident] => [:payload]
    channel :ack, [:@src, :dst, :ident]
    periodic :tock, 2
  }
  
  declare 
  def remember
    pipe <= pipe_in
    pipe_chan <~ join([pipe, tock]).map{|p, t| p }
  end
  
  declare
  def rcv
    ack <~ pipe_chan.map {|p| [p.src, p.dst, p.ident] }
  end

  declare 
  def done 
    apj = join [ack, pipe], [ack.ident, pipe.ident]
    pipe_sent <= apj.map {|a, p| p }
    pipe <- apj.map {|a, p| p }
  end
end


