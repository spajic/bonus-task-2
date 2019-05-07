class Mapper
  attr_reader :city_mapper, :bus_mapper, :buses_service_mapper, :trips_mapper, :service_mapper

  def initialize(city_mapper:, bus_mapper:, buses_service_mapper:, trips_mapper:, service_mapper:)
    @city_mapper = city_mapper
    @bus_mapper = bus_mapper
    @buses_service_mapper = buses_service_mapper
    @trips_mapper = trips_mapper
    @service_mapper = service_mapper
  end

  def map(array)
    array.each do |values|
      bus_values = values["bus"]

      bus_id = bus_mapper.add({ number: bus_values["number"], model: bus_values["model"] })

      service_values = bus_values["services"]
      service_values.map do |service|
        service_id = service_mapper.id(service)

        buses_service_mapper << { bus_id: bus_id, service_id: service_id }
      end

      trips_mapper << {
        from_id: city_mapper.id(values["from"]),
        to_id: city_mapper.id(values["to"]),
        bus_id: bus_id,
        start_time: values["start_time"],
        duration_minutes: values["duration_minutes"],
        price_cents: values["price_cents"]
      }
    end

    def clear_trips
      trips_mapper.clear
    end
  end
end
