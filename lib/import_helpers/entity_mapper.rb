class EntityMapper
  attr_reader :map

  def initialize
    @map = Hash.new { |h, k| h[k] = { id: h.length + 1, name: k } }
  end

  def add(value)
    map[value]
  end

  def id(value)
    map[value][:id]
  end

  def add_bunch(values)
    values.each { |value| add(value) }
  end
end
