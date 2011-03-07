require 'rubygems'
require 'uuid'
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
# for filesystem metadata.  The tree structure of the FS is embedded into
# the flat KVS namespace in the following way:
# * keys are fully-qualified path names
# * directories have arrays as their values, containing the directory contents 
#   (file or directory names)
# hence creating a file or directory involves three KVS operations:
# * looking up the parent path
# * updating the parent path
# * creating an entry for the file or directory

module KVSFS
  include FSProtocol
  include BasicKVS
  include SimpleNonce

  state {
    # in the KVS-backed implementation, we'll use the same routine for creating 
    # files and directories.
    scratch :check_parent_exists, [:reqid, :name, :path, :mtype, :data]
  }

  bootstrap do
    # replace with nonce reference?
    kvput <+ [[nil, '/', UUID.new, []]]
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
    check_parent_exists <= fscreate.map{ |c| [c.reqid, c.name, c.path, :create, c.data] }
    check_parent_exists <= fsmkdir.map{ |m| [m.reqid, m.name, m.path, :mkdir, nil] }
    check_parent_exists <= fsrm.map{ |m| [m.reqid, m.name, m.path, :rm, nil] }

    kvget <= check_parent_exists.map{ |c| [c.reqid, c.path] }    
    fsret <= check_parent_exists.map do |c|
      unless kvget_response.map{ |r| r.reqid}.include? c.reqid
        puts "not found" or [c.reqid, false, "parent path #{c.path} for #{c.name} does not exist"]
      end
    end

    dir_exists = join [check_parent_exists, kvget_response, nonce], [check_parent_exists.reqid, kvget_response.reqid]
    # update dir entry
    kvput <= dir_exists.map do |c, r, n|
      if c.mtype == :rm 
        [ip_port, c.path, n.ident, r.value.clone.reject{|item| item == c.name}]
      else 
          [ip_port, c.path, n.ident, r.value.clone.push(c.name)]
      end
    end
  
    kvput <= dir_exists.map do |c, r, n|
      case c.mtype
        when :mkdir
          [ip_port, terminate_with_slash(c.path) + c.name, c.reqid, []]
        when :create
          [ip_port, terminate_with_slash(c.path) + c.name, c.reqid, "LEAF"]
        when :rm
          # leak children!!
          puts "RM DIR"
          [ip_port, terminate_with_slash(c.path) + c.name, c.reqid, "TOMBSTONE"]
      end
    end

    fsret <= dir_exists.map{ |c, r| [c.reqid, true, nil] }
  end
  
  def terminate_with_slash(path)
    if path =~ /\/\z/
      return path
    else
      return path + "/"
    end
  end
end

