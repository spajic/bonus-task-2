class BusMapper
  attr_reader :map

  def initialize
    @map = Hash.new
  end

  def add(key)
    key_hash = generate_hash(key)

    map[key_hash] = key.merge({ id: map.length + 1 }) unless map[key_hash]

    id(key_hash)
  end

  def id(key_hash)
    map.dig(key_hash, :id)
  end

  private

  def generate_hash(key)
    key.hash
  end
end
