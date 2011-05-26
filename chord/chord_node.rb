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
