class BusesService < ActiveRecord::Base
  belongs_to :bus
  belongs_to :service
end
