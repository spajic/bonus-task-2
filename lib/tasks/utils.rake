# Наивная загрузка данных из json-файла в БД
# rake reload_json[fixtures/small.json]

require_relative File.join('..', 'import_helpers', 'helpers.rb')

task :reload_json, [:file_name] => :environment do |_task, args|
  json = JSON.parse(File.read(args.file_name))

  trips = []
  buses = {}
  buses_services_map = {}
  service_names = Set.new
  cities_names = Set.new

  json.each do |trip|
    cities_names << trip['from'] << trip['to']
    trip['bus']['services'].map { |service| service_names << service }

    bus_number = trip['bus']['number']
    bus_model = trip['bus']['model']

    buses[bus_number] = { number: bus_number, model: bus_model } unless buses[bus_number]

    unless buses_services_map[bus_number]
      buses_services_map[bus_number] = []
      buses_services_map[bus_number] = *trip['bus']['services']
    end

    trips << {
      from: trip['from'],
      to: trip['to'],
      bus_number: bus_number,
      start_time: trip['start_time'],
      duration_minutes: trip['duration_minutes'],
      price_cents: trip['price_cents']
    }
  end

  ActiveRecord::Base.transaction do
    City.delete_all
    Bus.delete_all
    Service.delete_all
    Trip.delete_all
    ActiveRecord::Base.connection.execute('delete from buses_services;')

    prepared_services = service_names.map { |name| { name: name } }
    ids = Service.import(prepared_services, validate: false).ids
    services_to_id = service_names.zip(ids).reduce({}) do |result, (name, id)|
      result[name] = id
      result
    end

    prepared_cities = cities_names.map { |name| { name: name } }
    ids = City.import(prepared_cities, validate: false).ids
    cities_to_ids = cities_names.zip(ids).reduce({}) do |result, (name, id)|
      result[name] = id
      result
    end

    buses_values = buses.values
    ids = Bus.import(buses_values, validate: false).ids
    buses_number_to_ids = buses_values.zip(ids).reduce({}) do |result, (bus_data, id)|
      result[bus_data[:number]] = id
      result
    end

    prepared_buses_services = buses_services_map.reduce([]) do |result, (bus_number, services)|
      bus_id = buses_number_to_ids[bus_number]

      services.each do |service|
        service_id = services_to_id[service]
        result << { bus_id: bus_id, service_id: service_id }
      end

      result
    end
    BusesService.import(prepared_buses_services, validate: false)

    prepared_trips = trips.map do |trip|
      {
        from_id: cities_to_ids[trip[:from]],
        to_id: cities_to_ids[trip[:to]],
        bus_id: buses_number_to_ids[trip[:bus_number]],
        start_time: trip[:start_time],
        duration_minutes: trip[:duration_minutes],
        price_cents: trip[:price_cents]
      }
    end

    Trip.import(prepared_trips, validate: false)
  end
end
