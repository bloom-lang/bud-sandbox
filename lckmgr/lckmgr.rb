require 'rubygems'
require 'bud'

module LockMgrProtocol
  state do
    interface input, :request_lock, [:xid, :resource]
    interface input, :end_xact, [:xid]
    interface output, :lock_status, [:xid, :resource] => [:status]
  end

end


module TwoPhaseLockMgr
  include LockMgrProtocol

  state do
    #table :lock, [:key] => [:xid]
    table :lock, [:key, :xid]
    table :pending, [:key, :xid]
    scratch :candidates, pending.schema
    scratch :chosen, lock.schema
  end

  bloom do
    pending <= request_lock{|l| [l.resource, l.xid]}
    temp :clutter <= (pending * end_xact).lefts(:xid => :xid)
    pending <- clutter
    lock <- clutter
    lock_status <= (request_lock * lock).lefts(:resource => :key, :xid => :xid) {|s| [s.xid, s.resource, :OK] }
    
    candidates <= pending do |p|
      unless lock.map{|l| l.key if l.xid != p.xid or (l.xid == p.xid and l.key == p.key)}.include? p.key
        p
      end
    end
    chosen <= candidates.group([candidates.key], choose(candidates.xid))
    lock <+ chosen
    lock_status <= chosen {|c| [c.xid, c.key, :OK]}
  end
end
