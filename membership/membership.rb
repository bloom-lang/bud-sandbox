require 'rubygems'
require 'bud'

module MembershipProto
  include BudModule

  state {
    interface input, :add_member, [:host] => [:ident]
    interface input, :my_id, [] => [:ident]
    table :member, [:host] => [:ident]
    table :local_id, [] => [:ident]
  }
end

module StaticMembership
  include MembershipProto

  declare
  def member_logic
    member <= add_member.map do |m|
      if @budtime == 0
        m
      else
        puts "REJECT #{m.inspect} @ #{@budtime}"
      end
    end
    local_id <= my_id.map{ |m| m if @budtime == 0 }
  end
end
