class JSONScannerHandler < ::Oj::ScHandler
  TRIPS_ENTITIES_CUTOFF = 1_000

  attr_reader :counter, :mapper, :importer

  def initialize(counter:, mapper:, importer:)
    @counter = counter
    @mapper = mapper
    @importer = importer
  end

  def hash_start
    counter.h_inc

    {}
  end

  def hash_end
    counter.h_dec
  end

  def hash_set(hash, key, value)
    hash[key] = value
  end

  def array_start
    counter.a_inc

    []
  end

  def array_end
    counter.a_dec
  end

  def array_append(array, value)
    array << value

    if array.count == TRIPS_ENTITIES_CUTOFF && counter.balanced?
      mapper.map(array)
      trips = mapper.trips_mapper
      importer.import_trips(trips)
      mapper.clear_trips

      array.clear
    end
  end

  def add_value(value)
    if value.any?
      mapper.map(value)

      trips = mapper.trips_mapper
      importer.import_trips(trips)
    end

    importer.trips_importer.finish

    city_mapper = mapper.city_mapper
    bus_mapper = mapper.bus_mapper
    buses_service_mapper = mapper.buses_service_mapper
    service_mapper = mapper.service_mapper

    importer.import_rest(city: city_mapper.map.values,
                         bus: bus_mapper.map.values,
                         buses_service: buses_service_mapper.to_a,
                         service: service_mapper.map.values)
  end

  def error(message, line, column)
    p "ERROR: #{message}"
  end
end
