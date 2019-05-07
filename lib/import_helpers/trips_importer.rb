class TripsImporter
  IMPORT_COMMAND = "copy trips (from_id, to_id, start_time, duration_minutes, price_cents, bus_id) from stdin with csv delimiter ';'"

  attr_accessor :connection

  def initialize
    @connection = ActiveRecord::Base.connection.raw_connection

    prepare
  end

  def import(trips)
    trips.each do |trip|
      trip_data = "#{trip[:from_id]};#{trip[:to_id]};#{trip[:start_time]};#{trip[:duration_minutes]};#{trip[:price_cents]};#{trip[:bus_id]}\n"

      connection.put_copy_data(trip_data)
    end
  end

  def finish
    @connection.put_copy_end
  end

  private

  def prepare
    @connection.exec(IMPORT_COMMAND)
  end
end
