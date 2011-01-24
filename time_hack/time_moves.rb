module TimeMoves
  def state
    super
    periodic :ticc, 1
  end
end

