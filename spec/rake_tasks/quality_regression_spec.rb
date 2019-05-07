# frozen_string_literal: true

require "rails_helper"
require "rake"

MAX_TIME_EXECUTION = 0.3

RSpec.describe "Quality regression spec", quality_regression: true do
  describe "loading data" do
    before(:all) do
      Rake.application.rake_require "tasks/utils"
      Rake::Task.define_task(:environment)
    end

    it "executes on time" do
      expect(time { Rake::Task["reload_json"].invoke("fixtures/small.json") }).to be < MAX_TIME_EXECUTION
    end
  end
end
