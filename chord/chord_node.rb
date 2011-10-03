require 'rubygems'
require 'bud'

# basic state and macros
module ChordNode  
  state do
    table :me, [] => [:start, :pred_id, :pred_addr]
    table :finger, [:index] => [:start, :hi, :succ, :succ_addr]
    table :localkeys
  end
  
  def in_range(key, lo, hi, inclusive_lo = false, inclusive_hi=false)
    return false if lo.nil? or hi.nil? or key.nil?
    if hi >= lo
      return ((inclusive_lo ? key >= lo : key > lo) and (inclusive_hi ? key <= hi : key < hi))
    else
      return (((inclusive_lo ? key >= lo : key > lo) and key < @maxkey) \
              or (key >= 0 and (inclusive_hi ? key <= hi : key < hi)))
    end
  end
  
  def log2(x)
    Math.log(x)/Math.log(2)
  end
  
  def at_successor(key)
    return false if key.nil? or me.first.nil? or me.first.start.nil? \
                    or finger[[0]].nil? or finger[[0]].succ.nil?
    return in_range(key, me.first.start, finger[[0]].succ, false, true)
  end
  
  def at_local(key)
    return (me.first and me.first.start and key == me.first.start) ? true : false
  end
  
  def at_finger(key, index)
    return false if finger[[index]].nil? or finger[[index]].start.nil?
    return in_range(key % @maxkey, finger[[index]].start, finger[[index]].hi, true, false)
  end
end

# utility module to get id and predecessor info from one's successor
module ChordSuccPred
  state do
    channel :sp_req, [:@to, :from, :hops]
    channel :sp_resp, [:@to, :from, :hops] + me.cols
    interface :input, :succ_pred_req, [:to, :from, :hops]
    interface :output, :succ_pred_resp, sp_resp.cols[1..-1]
    interface :output, :succ_pred_timeout, sp_req.schema
    table :pending, [:to, :from, :hops, :save]
    periodic :sp_timeout, 5
  end
  
  bloom do
    # cache pending requests to catch timeouts.
    # note that for multi-hop requests we only timeout the first hop    
    sp_req <~ succ_pred_req
    pending <+ succ_pred_req {|r| (r + [true]) }
        
    # if timer fires and save is true, update save to false
    pending <+ (pending * sp_timeout).lefts { |p| p[0..-2] + [false] if p.save }
    # else notify on succ_pred_timeout interface
    succ_pred_timeout <= (pending * sp_timeout).lefts { |p| p[0..-2] unless p.save }
    # in either case delete old pending tuple
    pending <- (pending * sp_timeout).lefts { |p| p }

    # respond to request
    sp_resp <~ sp_req { |s| [s.from, ip_port, s.hops] + me.first unless me.first.nil? }
    succ_pred_resp <= sp_resp { |s| s[1..-1] }
    # remove from cache upon response
    pending <- (sp_resp * pending).rights(:from => :to)
    
    
    # and forward along if hops > 1
    sp_req <~ sp_req do |s| 
      [finger[[0]].succ_addr, s.from, s.hops - 1] unless finger[[0]].nil? or s.hops <= 1
    end
  end
end
