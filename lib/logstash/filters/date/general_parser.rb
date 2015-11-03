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
    class GeneralParser < BaseDateParser

      attr_reader :parsers

      ENGLISH_LOCALE = "en".freeze
      ENGLISH_US_CODE = "en-US".freeze

      def initialize(sprintf, timezone=nil)
        super(timezone)
        @parsers  = []
        @sprintf  = sprintf
      end

      def setup(locale, pattern)
        @parsers = []

        parser = DateTimeFormat.forPattern(pattern).withDefaultYear(Time.new.year)
        parser = configure_offset(parser)
        parser = parser.withLocale(locale) if locale

        format_has_year = pattern.match(/y|Y/)

        if @sprintf
          @parsers << lambda { |date , tz|
            parser.withZone(DateTimeZone.forID(tz)).parseMillis(date)
          }
        else
          @parsers << lambda { |date|
            return parser.parseMillis(date) if format_has_year
            now = Time.now
            now_month = now.month
            result = parser.parseDateTime(date)
            event_month = result.month_of_year.get

            if (event_month == now_month)
              result.with_year(now.year)
            elsif (event_month == 12 && now_month == 1)
              result.with_year(now.year-1)
            elsif (event_month == 1 && now_month == 12)
              result.with_year(now.year+1)
            else
              result.with_year(now.year)
            end.get_millis
          }
        end

        #Include a fallback parser to english when default locale is non-english
        if !locale && ENGLISH_LOCALE != default_locale && need_fallback_for?(pattern)
          en_parser = parser.withLocale(locale_for(ENGLISH_US_CODE))
          parsers << lambda { |date| en_parser.parseMillis(date) }
        end
      end


      private

      def locale_for(code)
        Locale.forLanguageTag(code)
      end

      def default_locale
        Locale.getDefault().getLanguage()
      end

      def need_fallback_for?(pattern)
        (pattern.include?("MMM") || pattern.include?("E"))
      end

    end
  end
end
