# Наивная загрузка данных из json-файла в БД
# rake reload_json[fixtures/small.json]

require 'oj'

Dir[Rails.root.join("lib", "import_helpers", "**", "*.rb")].sort.each { |f| require f }

task :heavy_load_json, [:file_name] => :environment do |_task, args|
  service_mapper = EntityMapper.new
  service_mapper.add_bunch(Service::SERVICES)

  ent_counter = EntityCounter.new

  mapper = Mapper.new(city_mapper: EntityMapper.new,
                      bus_mapper: BusMapper.new,
                      buses_service_mapper: Set.new,
                      trips_mapper: [],
                      service_mapper: service_mapper)

  importer = Importer.new(city_importer: EntityImporter.new(City),
                          service_importer: EntityImporter.new(Service),
                          bus_importer: EntityImporter.new(Bus),
                          bus_service_importer: EntityImporter.new(BusesService),
                          trips_importer: TripsImporter.new)

  File.open(args.file_name) do |f|
    json_scanner = JSONScannerHandler.new(counter: ent_counter, mapper: mapper, importer: importer)

    Oj.sc_parse(json_scanner, f)
  end

  memory_consumption
end
