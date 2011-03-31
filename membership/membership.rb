require 'rubygems'
require 'bud'

module MembershipProtocol
  state do
    interface input, :add_member, [:host] => [:ident]
    interface input, :my_id, [] => [:ident]
    table :member, [:host] => [:ident]
    table :local_id, [] => [:ident]
  end
end

# This is a simple membership protocol that only allows members to be added to
# the group before the first Bud timestep.
module StaticMembership
  include MembershipProtocol

  bloom do
    member <= add_member do |m|
      if @budtime == 0
        m
      else
        raise "REJECT #{m.inspect} @ #{@budtime}"
      end
    end
    local_id <= my_id {|m| m if @budtime == 0}
  end
end
