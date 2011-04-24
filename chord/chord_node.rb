require 'rubygems'
require 'bud'

module ChordNode  
  state do
    table :finger, [:index] => [:start, :hi, :succ, :succ_addr]
    table :me, [] => [:start]
    table :localkeys
  end
  
  def in_range(key, lo, hi, inclusive_hi=false)
    if hi > lo
      return (key > lo and (inclusive_hi ? key <= hi : key < hi))
    else
      return ((key > lo and key < @maxkey) or (key >= 0 and (inclusive_hi ? key <= hi : key < hi)))
    end
  end
  
  def log2(x)
    Math.log(x)/Math.log(2)
  end
  
  def at_successor(event, me, fing)
    fing.index == 0 and in_range(event.key, me.start, fing.succ, true)
  end
end
