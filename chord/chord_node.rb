require 'rubygems'
require 'bud'

module ChordNode  
  state do
    table :finger, [:index] => [:start, :hi, :succ, :succ_addr]
    table :me, [] => [:start, :pred_id, :pred_addr]
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
    return false if key.nil? or me.nil? or me.length == 0 or me.first.start.nil? or finger[[0]].nil? or finger[[0]].succ.nil?
    return in_range(key, me.first.start, finger[[0]].succ, false, true)
  end
end
