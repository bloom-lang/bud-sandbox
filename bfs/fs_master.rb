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

  state do
    interface input, :fsls, [:reqid, :path]
    interface input, :fscreate, [] => [:reqid, :name, :path, :data]
    interface input, :fsmkdir, [] => [:reqid, :name, :path]
    interface input, :fsrm, [] => [:reqid, :name, :path]
    interface output, :fsret, [:reqid, :status, :data]
  end
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
  include TimestepNonce
  include AggAssign

  state do
    # in the KVS-backed implementation, we'll use the same routine for creating 
    # files and directories.
    scratch :check_parent_exists, [:reqid, :name, :path, :mtype, :data]
    scratch :check_is_empty, [:reqid, :orig_reqid, :name]
    scratch :can_remove, [:reqid, :orig_reqid, :name]

  end

  bootstrap do
    # replace with nonce reference?
    kvput <= [[nil, '/', gen_id, []]]
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
        puts "not found #{c.path}" or [c.reqid, false, "parent path #{c.path} for #{c.name} does not exist"]
      end
    end

    # if the block above had no rows, dir_exists will have rows.
    dir_exists = join [check_parent_exists, kvget_response, nonce], [check_parent_exists.reqid, kvget_response.reqid]

    check_is_empty <= join([fsrm, nonce]).map{|m, n| [n.ident, m.reqid, terminate_with_slash(m.path) + m.name] }
    kvget <= check_is_empty.map{|c| [c.reqid, c.name] }
    can_remove <= join([kvget_response, check_is_empty], [kvget_response.reqid, check_is_empty.reqid]).map do |r, c|
      [c.reqid, c.orig_reqid, c.name] if r.value.length == 0
    end

    fsret <= dir_exists.map do |c, r, n|
      if c.mtype == :rm
        unless can_remove.map{|can| can.orig_reqid}.include? c.reqid
          [c.reqid, false, "directory #{} not empty"]
        end
      end
    end
      
    # update dir entry
    # note that it is unnecessary to ensure that a file is created before its corresponding
    # directory entry, as both inserts into :kvput below will co-occur in the same timestep.
    kvput <= dir_exists.map do |c, r, n|
      if c.mtype == :rm 
        if can_remove.map{|can| can.orig_reqid}.include? c.reqid
          [ip_port, c.path, n.ident, r.value.clone.reject{|item| item == c.name}]
        end
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
      end
    end

    # delete entry -- if an 'rm' request, 
    kvdel <= dir_exists.map do |c, r, n| 
      if can_remove.map{|can| can.orig_reqid}.include? c.reqid
        [terminate_with_slash(c.path) + c.name, c.reqid] 
      end
    end

    # report success if the parent directory exists (and there are no errors)
    # were there errors, we'd never reach fixpoint.
    fsret <= dir_exists.map do |c, r| 
      unless c.mtype == :rm and ! can_remove.map{|can| can.orig_reqid}.include? c.reqid
        [c.reqid, true, nil] 
      end
    end

  end

  def terminate_with_slash(path)
    return path[-1..-1] == '/' ? path : path + '/'
  end
  
end

