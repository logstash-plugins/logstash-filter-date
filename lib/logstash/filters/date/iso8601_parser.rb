require "logstash/filters/date/base_parser"
require "java"
java_import java.util.Locale
java_import org.joda.time.format.ISODateTimeFormat
java_import org.joda.time.DateTimeZone
java_import org.joda.time.format.DateTimeParser
java_import org.joda.time.format.DateTimeFormat
java_import org.joda.time.format.DateTimeFormatterBuilder

module LogStash
  module Filters
    class Iso8601Parser < BaseDateParser

      attr_reader :parsers

      def initialize(timezone=nil)
        super(timezone)
        @parsers  = []
      end

      def setup
        parsers = []
        parsers << configure_offset(ISODateTimeFormat.dateTimeParser)
        parsers << configure_offset(setup_almost_iso_parsers)
        parsers.each do |parser|
          @parsers << lambda { |date| parser.parseMillis(date) }
        end
      end

      private

      def setup_almost_iso_parsers
        almostISOparsers = [
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSSZ").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS").getParser()
        ].to_java(DateTimeParser)
        DateTimeFormatterBuilder.new.append( nil, almostISOparsers ).toFormatter()
      end

    end
  end
end
