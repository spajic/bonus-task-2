class EntityImporter
  attr_reader :klass

  def initialize(klass)
    @klass = klass
  end

  def import(map)
    klass.import(map, validate: false)
  end
end
