class TripsController < ApplicationController
  def index
    @from, @to, @trips = ::Queries::TripsQuery.call(params[:from], params[:to])
  end
end
