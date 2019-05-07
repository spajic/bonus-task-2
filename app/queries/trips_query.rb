module Queries
  class TripsQuery
    class << self
      def call(from, to)
        from = city(from)
        to = city(to)
        trip = Trip
                 .where(from: from, to: to)
                 .order(:start_time)
                 .preload(bus: :services)

        [from, to, trip]
      end

      private

      def city(name)
        City.find_by_name!(name)
      end
    end
  end
end
