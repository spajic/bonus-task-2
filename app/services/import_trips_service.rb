# frozen_string_literal: true

require 'yajl/ffi'
require 'json/streamer'
require 'piperator'

class ImportTripsService
  BATCH_SIZE = 1000
  TRIPS_COMMAND = "copy trips (from_id, to_id, start_time, duration_minutes, price_cents, bus_id) from stdin with csv delimiter ';'"

  def self.load(file_name)
    new(file_name).load
  end

  attr_accessor :json, :cities, :buses, :services, :buses_services, :file_name, :conn

  def initialize(file_name)
    @file_name = file_name
    @conn = ActiveRecord::Base.connection.raw_connection
    @cities = {}
    @buses = {}
    @services = {}
    @buses_services = []
  end

  def load
    clean_db!
    load_services
    load_trips
    load_cities
    load_buses
    load_buses_services
  end

  def clean_db!
    BusesService.delete_all
    Trip.delete_all
    City.delete_all
    Bus.delete_all
    Service.delete_all
  end

  def load_trips
    disable_indices_for(:trips)

    import = proc { |enumerable| enumerable.lazy.each { |trip| import(trip) } }

    streaming_json = proc do |file_name|
      Enumerator.new do |yielder|
        io = File.open(file_name, 'r')
        streamer = Json::Streamer.parser(file_io: io, event_generator: Yajl::FFI::Parser.new)
        streamer.get(nesting_level: 1).each do |chunk|
          yielder << chunk
        end
      end
    end

    ActiveRecord::Base.transaction do
      Piperator
        .pipe(streaming_json)
        .pipe(import)
        .call(file_name)
    end

    enable_indices_for(:trips)
  end

  def import(trip)
    from_id, to_id = collect_cities_from(trip)
    bus_id = collect_buses_from(trip)

    conn.copy_data TRIPS_COMMAND do
      conn.put_copy_data("#{from_id};#{to_id};#{trip['start_time']};#{trip['duration_minutes']};#{trip['price_cents']};#{bus_id}\n")
    end
  end

  def collect_cities_from(trip)
    from_id = cities[trip['from']]
    unless from_id
      from_id = cities.size + 1
      cities[trip['from']] = from_id
    end

    to_id = cities[trip['to']]
    unless to_id
      to_id = cities.size + 1
      cities[trip['to']] = to_id
    end

    [from_id, to_id]
  end

  def collect_buses_from(trip)
    bus = trip['bus']
    composite_key = Array[bus['number'], bus['model']]
    bus_id = buses[composite_key]
    unless bus_id
      bus_id = buses.size + 1
      buses[composite_key] = bus_id
    end

    bus['services'].each { |service| buses_services << Array[bus_id, services[service]] }
    bus_id
  end

  def disable_indices_for(table)
    ActiveRecord::Base.connection.execute(<<-SQL.squish)
      UPDATE pg_index
      SET indisready=false
      WHERE indrelid = (
        SELECT oid
        FROM pg_class
        WHERE relname='#{table}'
      );
    SQL
  end

  def enable_indices_for(table)
    ActiveRecord::Base.connection.execute(<<-SQL.squish)
      UPDATE pg_index
      SET indisready=true
      WHERE indrelid = (
        SELECT oid
        FROM pg_class
        WHERE relname='#{table}'
      );
    SQL
    binding
    ActiveRecord::Base.connection.execute("REINDEX TABLE #{table}")
  end

  def load_services
    selection = Service.import(%i[name], Service::SERVICES.product, returning: :name)
    ids = selection.ids
    selection.results.each.with_index { |attr, i| services[attr] = ids[i] }
  end

  def load_cities
    City.import(cities.map { |city, id| { name: city, id: id } }, options)
  end

  def load_buses
    disable_indices_for :buses
    columns = %i[number model id].freeze
    values = buses.map { |k, v| k << v }
    Bus.import(columns, values, options)
    enable_indices_for :buses
  end

  def load_buses_services
    columns = %i[bus_id service_id].freeze
    buses_services.uniq!
    BusesService.import(columns, buses_services, options)
  end

  def options
    {
      batch_size: BATCH_SIZE
    }
  end
end
