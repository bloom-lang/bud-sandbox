require 'rubygems'
require 'bud'
require 'bfs/bfs_client'
require 'kvs/kvs'

module FSProtocol
  def state
    super
    interface input, :fsls, ['reqid', 'path']
    interface input, :fscreate, [], ['reqid', 'name', 'path', 'data']
    interface input, :fsrm, [], ['reqid', 'name', 'path']
    interface input, :fmkdir, [], ['reqid', 'name', 'path']
  
    interface output, :fsret, ['reqid', 'status', 'data']
  end
end

module FS 
  include FSProtocol
  include Anise
  annotator :declare

  def bootstrap
    file <+ [[0, '/']]
    super
  end

  def state
    super
    table :file, ['fid', 'name']
    table :dir, ['dir', 'contained']
    table :fqpath, ['fid', 'path']

    scratch :cr, ['reqid', 'loc', 'name']
    scratch :lookup, ['reqid', 'path']
    scratch :result, ['reqid', 'fid']
  end

  declare
  def view
    fqpath <= [[0, '/']]
    fqpath <= join([dir, fqpath, file], [dir.dir, fqpath.fid], [dir.contained, file.fid]).map do |d, p, f|
      puts "path now " + p.inspect or [f.fid, p.path + '/' + f.name] unless f.name == '/'
    end 

  end
  
  declare
  def elless
    lookup <= fsls

    dataj = join([result, dir, file], [result.fid, dir.dir], [dir.contained, file.fid])
    fsret <= join([result, dir, file], [result.fid, dir.dir], [dir.contained, file.fid]).map do |r, d, f| 
      puts "DATA: " + f.name or [r.reqid, true, f.name] 
    end

    fsret <= fsls.map do |l|
      unless result.map{|r| r.reqid}.include? l.reqid
        [l.reqid, false]
      end
    end
  end

  declare 
  def looker
    lookj = join([lookup, fqpath], [lookup.path, fqpath.path])
    result <= lookj.map{|l, p| puts "lookj" or [l.reqid, p.fid] } 
  end
  
  declare
  def create
    lookup <= fscreate.map{|c| [c.reqid, c.path] }

    fsret <= fscreate.map do |l|
      unless result.map{|r| r.reqid}.include? fsls.reqid
        [l.reqid, false]
      end
    end
      
    
    
  end
end
