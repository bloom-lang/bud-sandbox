require 'rubygems'
require 'bud'

module ChordNode
  include Anise
  annotator :declare

  def state
    super
    interface input, :find_event, ['key', 'from']
    interface output, :closest, ['key'], ['index', 'start', 'hi', 'succ', 'succ_addr']
    table :finger, ['index'], ['start', 'hi', 'succ', 'succ_addr']
    table :me, [], ['start']
    scratch :candidate, ['key', 'index', 'start', 'hi', 'succ', 'succ_addr']
    table :localkeys, ['key'], ['val']
  end

  def in_range(key, lo, hi, inclusive_hi=false)
    if hi > lo
      return (key > lo and (inclusive_hi ? key <= hi : key < hi))
    else
      return ((key > lo and key < @maxkey) or (key >= 0 and (inclusive_hi ? key <= hi : key < hi)))
    end
  end


  declare
  def node_views
    # for each find_event for an id, find index of the closest finger
    # start by finding all fingers with IDs between this node and the search key
    candidate <= join([find_event,finger,me]).map do |e,f,m|
                   [e.key, f.index, f.start, f.hi, f.succ, f.succ_addr] if in_range(f.succ, m.start, e.key)
                 end
    # now for each key pick the highest-index candidate; it's the closest
    closest <= candidate.argmax([candidate.key], candidate.index)
    # stdio <~ closest.map {|m| ["closest@#{me.first.start.to_s}: #{m.inspect}"]}
  end
end
