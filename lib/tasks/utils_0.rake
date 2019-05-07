# Наивная загрузка данных из json-файла в БД
# rake reload_json[fixtures/small.json]

require_relative File.join('..', 'import_helpers', 'helpers.rb')

task :reload_json_2, [:file_name] => :environment do |_task, args|
  json = JSON.parse(File.read(args.file_name))

  cities_hash = Hash.new { |hash, key| hash[key] = City.create(name: key) }
  services_hash = Hash.new { |hash, key| hash[key] = Service.create(name: key) }
  buses_number_to_id = {}
  buses_services = []
  trips = []

  ActiveRecord::Base.transaction do
    time_profiler("DELETE DATA") do
      City.delete_all
      Bus.delete_all
      Service.delete_all
      Trip.delete_all
      ActiveRecord::Base.connection.execute('delete from buses_services;')
    end

    json.each do |trip|
      from = cities_hash[trip['from']]
      to = cities_hash[trip['to']]

      bus_number = trip['bus']['number']
      bus_model = trip['bus']['model']

      services = trip['bus']['services'].map { |service| services_hash[service] }

      bus_id = buses_number_to_id.fetch(bus_number) do |key|
        bus_id = BusForInsert.create!(number: bus_number, model: bus_model).id

        buses_services.push(*services.map do |service|
          { bus_id: bus_id, service_id: service.id }
        end)

        buses_number_to_id[key] = bus_id
      end

      trips << {
        from_id: from.id,
        to_id: to.id,
        bus_id: bus_id,
        start_time: trip['start_time'],
        duration_minutes: trip['duration_minutes'],
        price_cents: trip['price_cents']
      }
    end

    BusesService.import(buses_services, validate: false)

    Trip.import(trips, validate: false)
  end
end
