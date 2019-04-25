# frozen_string_literal: true

require 'yajl/ffi'
require 'json/streamer'
require 'piperator'
require 'oj'

class ImportTripsService < Oj::ScHandler
  BATCH_SIZE = 1000
  TRIPS_COMMAND = "copy trips (from_id, to_id, start_time, duration_minutes, price_cents, bus_id) from stdin with csv delimiter ';'"

  attr_accessor :is_bus, :is_service, :nesting, :last_key, :buses,
                :services, :buses_services, :cities, :conn, :trip, :bus, :service

  def self.load(file_name)
    new(file_name).load
  end

  def initialize(file_name)
    @is_bus = false
    @is_service = false
    @nesting = 0

    @trip = {}
    @bus = {}
    @service = []
    @cities = {}
    @buses = {}
    @services = {}
    @buses_services = []

    @file_name = file_name
    @conn = ActiveRecord::Base.connection.raw_connection
  end

  def hash_key(key)
    @last_key = key
    @is_bus = true if key == 'bus'
    @is_service = true if key == 'services'
  end

  def hash_start
    @nesting += 1
    @service.clear
  end

  def hash_set(_h, _key, value)
    bus[@last_key] = value if is_bus
    trip[@last_key] = value unless is_bus
  end

  def hash_end
    @nesting -= 1
    @is_bus = false if @is_bus

    push_to_db if @nesting.zero?
  end

  def array_append(_a, value)
    service << value if @is_service
  end

  def array_end
    @is_service = false if @is_service
  end

  def load
    clean_db!
    load_services
    load_trips
    load_cities
    load_buses
    load_buses_services
  end

  private

  def clean_db!
    BusesService.delete_all
    Trip.delete_all
    City.delete_all
    Bus.delete_all
    Service.delete_all
  end

  def push_to_db
    from_id = fetch_city_id(trip['from'])
    to_id = fetch_city_id(trip['to'])
    bus_id = fetch_bus_id

    conn.put_copy_data("#{from_id};#{to_id};#{trip['start_time']};#{trip['duration_minutes']};#{trip['price_cents']};#{bus_id}\n")
  end

  def load_trips
    disable_indices_for(:trips)
    conn.copy_data TRIPS_COMMAND do
      io = File.open(@file_name, 'r')
      Oj.sc_parse(self, io)
    end
    enable_indices_for(:trips)
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

    ActiveRecord::Base.connection.execute("REINDEX TABLE #{table}")
  end

  def load_services
    selection = Service.import(%i[name], Service::SERVICES.product, returning: :name)
    ids = selection.ids
    selection.results.each.with_index { |attr, i| services[attr] = ids[i].to_i }
  end

  def load_cities
    disable_indices_for :cities
    City.import(cities.map { |city, id| { name: city, id: id } }, options)
    enable_indices_for :cities
  end

  def load_buses
    disable_indices_for :buses
    columns = %i[number model id].freeze
    values = buses.map { |k, v| k << v }
    Bus.import(columns, values, options)
    enable_indices_for :buses
  end

  def load_buses_services
    disable_indices_for :buses_services
    columns = %i[bus_id service_id].freeze
    buses_services.uniq!

    BusesService.import(columns, buses_services, options)
    enable_indices_for :buses_services
  end

  def options
    {
      batch_size: BATCH_SIZE
    }
  end

  def fetch_city_id(city)
    id = cities[city]
    unless id
      id = cities.size + 1
      cities[city] = id
    end
    id
  end

  def fetch_bus_id
    composite_key = [bus['number'], bus['model']]
    bus_id = buses[composite_key]
    unless bus_id
      bus_id = buses.size + 1
      buses[composite_key] = bus_id
    end
    service.each { |s| buses_services << [bus_id, services[s]] }
    bus_id
  end
end
