# frozen_string_literal: true

module Helpers
  module Quality
    def time(&block)
      Benchmark.realtime do
        block.call
      end
    end
  end
end
