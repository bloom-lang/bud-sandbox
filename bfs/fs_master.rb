require 'rubygems'
require 'bud'
require 'bfs/bfs_client'
require 'kvs/kvs'
require 'ordering/serializer'
require 'ordering/assigner'
require 'ordering/nonce'

module FSProtocol
  include BudModule

  state {
    interface input, :fsls, [:reqid, :path]
    interface input, :fscreate, [] => [:reqid, :name, :path, :data]
    interface input, :fsrm, [] => [:reqid, :name, :path]
    interface input, :fsmkdir, [] => [:reqid, :name, :path]
  
    interface output, :fsret, [:reqid, :status, :data]
  }
end

module KVSFS
  include FSProtocol
  include BasicKVS

  def bootstrap
    super
    # replace with nonce reference?
    kvput <+ [[@ip_port, '/', 23646, []]]
  end
  
  declare 
  def elles
    kvget <= fsls.map{ |l| [l.reqid, l.path] } 
    fsret <= join([kvget_response, fsls], [kvget_response.reqid, fsls.reqid]).map{ |r, i| [r.reqid, true, r.value] }
    fsret <= fsls.map do |l|
      unless kvget_response.map{ |r| r.reqid}.include? l.reqid
        [l.reqid, false, nil]
      end
    end
  end

  declare
  def create
    kvget <= fscreate.map{ |c| [c.reqid, c.path] }    
    fsret <= fscreate.map do |c|
      unless kvget_response.map{ |r| r.reqid}.include? c.reqid
        [c.reqid, false, nil]
      end
    end

    dir_exists = join [fscreate, kvget_response], [fscreate.reqid, kvget_response.reqid]
    # update dir entry
    kvput <= dir_exists.map do |c, r|
      puts "DO it with #{r.inspect}" or [@ip_port, c.path, c.reqid+1, r.value.clone.push(c.name)]
    end

    kvput <= dir_exists.map do |c, r|
      [@ip_port, c.path.sub("/", "") + '/' + c.name, c.reqid, "DATA"]
    end
    fsret <= dir_exists.map{ |c, r| [c.reqid, true, nil] }
  end

  declare
  def mkdir
    kvget <= fsmkdir.map{ |m| [m.reqid, m.path] }
    fsret <= fsmkdir.map do |c|
      unless kvget_response.map{ |r| r.reqid}.include? c.reqid
        [c.reqid, false, nil]
      end
    end

    mkdir_exists = join [fsmkdir, kvget_response], [fsmkdir.reqid, kvget_response.reqid]
    kvput <= mkdir_exists.map do |c, r|
      puts "DO it with #{r.inspect}" or [@ip_port, c.path, c.reqid+1, r.value.clone.push(c.name)]
    end
    kvput <= mkdir_exists.map do |c, r|
      [@ip_port, c.path.sub("/", "") + '/' + c.name, c.reqid, []]
    end

    fsret <= mkdir_exists.map{ |c, r| [c.reqid, true, nil] }
  end
end


####### 
# fold; aborted 'pure' FS below

module FS 
  include FSProtocol
  include Serializer
  include SimpleNonce

  def bootstrap
    file <+ [[0, '/']]
    super
  end

  state {
    table :file, [:fid, :name]
    table :dir, [:dir, :contained]
    table :fqpath, [:fid, :path]

    scratch :cr, [:reqid, :loc, :name]
    scratch :lookup, [:reqid, :path]
    scratch :result, [:reqid, :fid]
  }

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
    enqueue <= fscreate.map{|c| [c.reqid, c.path] } 

    #create_attempts = join [

    fsret <= fscreate.map do |l|
      unless result.map{|r| r.reqid}.include? fsls.reqid
        [l.reqid, false]
      end
    end
      
    
    
  end
end
