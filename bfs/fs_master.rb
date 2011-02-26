require 'rubygems'
require 'bud'
require 'bfs/bfs_client'
require 'kvs/kvs'
require 'ordering/serializer'
require 'ordering/assigner'
require 'ordering/nonce'


# FSProtocol is the basic filesystem input/output contract.

module FSProtocol
  include BudModule

  state {
    interface input, :fsls, [:reqid, :path]
    interface input, :fscreate, [] => [:reqid, :name, :path, :data]
    interface input, :fsmkdir, [] => [:reqid, :name, :path]
    interface input, :fsrm, [] => [:reqid, :name, :path]
    interface output, :fsret, [:reqid, :status, :data]
  }
end


# KVSFS is an implementation of FSProtocol that uses a key-value store 
# for filesystem metadata.

module KVSFS
  include FSProtocol
  include BasicKVS
  

  state {
    # in the KVS-backed implementation, we'll use the same routine for creating 
    # files and directories.
    scratch :make, [:reqid, :name, :path, :mtype, :data]
  }

  bootstrap do
    # replace with nonce reference?
    kvput <+ [[ip_port, '/', 23646, []]]
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
    make <= fscreate.map{ |c| [c.reqid, c.name, c.path, :create, c.data] }
    make <= fsmkdir.map{ |m| [m.reqid, m.name, m.path, :mkdir, nil] }
    make <= fsrm.map{ |m| [m.reqid, m.name, m.path, :rm, nil] }

    #stdio <~ make.map{|m| ["MAKE: #{m.inspect}"] }

    kvget <= make.map{ |c| [c.reqid, c.path] }    
    fsret <= make.map do |c|
      unless kvget_response.map{ |r| r.reqid}.include? c.reqid
        puts "not found" or [c.reqid, false, "parent path #{c.path} for #{c.name} does not exist"]
      end
    end

    dir_exists = join [make, kvget_response], [make.reqid, kvget_response.reqid]
    # update dir entry
    kvput <= dir_exists.map do |c, r|
      if c.mtype == :rm 
        [ip_port, c.path, c.reqid+1, r.value.clone.reject{|item| item == c.name}]
      else 
          [ip_port, c.path, c.reqid+1, r.value.clone.push(c.name)]
      end
    end
  
    kvput <= dir_exists.map do |c, r|
      case c.mtype
        when :mkdir
          [ip_port, clean_path(c.path) + c.name, c.reqid, []]
        when :create
          [ip_port, clean_path(c.path) + c.name, c.reqid, "LEAF"]
        when :rm
          # leak children!!
          puts "RM DIR"
          [ip_port, clean_path(c.path) + c.name, c.reqid, "TOMBSTONE"]
      end
    end

    fsret <= dir_exists.map{ |c, r| [c.reqid, true, nil] }
  end
  
  def clean_path(path)
    if path =~ /\/\z/
      return path
    else
      return path + "/"
    end
  end
end

