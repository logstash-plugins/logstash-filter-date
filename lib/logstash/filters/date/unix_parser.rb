require "logstash/filters/date/base_parser"

module LogStash
  module Filters
    class UnixParser < BaseDateParser

      attr_reader :parsers

      def initialize
        super
        @parsers  = []
      end

      def setup
        @parsers = []
        @parsers << lambda do |date|
          raise "Invalid UNIX epoch value '#{date}'" unless /^\d+(?:\.\d+)?$/ === date || date.is_a?(Numeric)
          (date.to_f * 1000).to_i
        end
      end

    end
  end
end
