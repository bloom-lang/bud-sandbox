require 'rubygems'
require 'bud'

module MembershipProto
  def state
    super
    interface input, :add_member, ['host', 'ident']
    table :member, ['host', 'ident']
  end
end

module StaticMembership
  include MembershipProto
  include Anise
  annotator :declare

  declare 
  def member_logic
    member <= add_member.map{ |m| m if @budtime == 0 } 
  end
  
end
