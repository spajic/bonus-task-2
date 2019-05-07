class Importer
  attr_reader :city_importer, :service_importer, :bus_importer, :bus_service_importer, :trips_importer

  def initialize(city_importer:, service_importer:, bus_importer:, bus_service_importer:, trips_importer:)
    @city_importer = city_importer
    @service_importer = service_importer
    @bus_importer = bus_importer
    @bus_service_importer = bus_service_importer
    @trips_importer = trips_importer
  end

  def import_trips(values)
    trips_importer.import(values)
  end

  def import_rest(city:, bus:, buses_service:, service:)
    city_importer.import(city)
    bus_service_importer.import(buses_service)
    service_importer.import(service)

    bus_importer.import(bus)
  end
end
