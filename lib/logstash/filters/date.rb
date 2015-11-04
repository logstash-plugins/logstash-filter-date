# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/filters/date/iso8601_parser"
require "logstash/filters/date/general_parser"

require "java"
java_import java.util.Locale

# The date filter is used for parsing dates from fields, and then using that
# date or timestamp as the logstash timestamp for the event.
#
# For example, syslog events usually have timestamps like this:
# [source,ruby]
#     "Apr 17 09:32:01"
#
# You would use the date format `MMM dd HH:mm:ss` to parse this.
#
# The date filter is especially important for sorting events and for
# backfilling old data. If you don't get the date correct in your
# event, then searching for them later will likely sort out of order.
#
# In the absence of this filter, logstash will choose a timestamp based on the
# first time it sees the event (at input time), if the timestamp is not already
# set in the event. For example, with file input, the timestamp is set to the
# time of each read.
#class LogStash::Filters::Date < LogStash::Filters::Base

module LogStash
  module Filters
    class Date  < LogStash::Filters::Base

      config_name "date"

      # Specify a time zone canonical ID to be used for date parsing.
      # The valid IDs are listed on the http://joda-time.sourceforge.net/timezones.html[Joda.org available time zones page].
      # This is useful in case the time zone cannot be extracted from the value,
      # and is not the platform default.
      # If this is not specified the platform default will be used.
      # Canonical ID is good as it takes care of daylight saving time for you
      # For example, `America/Los_Angeles` or `Europe/Paris` are valid IDs.
      # This field can be dynamic and include parts of the event using the `%{field}` syntax
      config :timezone, :validate => :string

      # Specify a locale to be used for date parsing using either IETF-BCP47 or POSIX language tag.
      # Simple examples are `en`,`en-US` for BCP47 or `en_US` for POSIX.
      #
      # The locale is mostly necessary to be set for parsing month names (pattern with `MMM`) and
      # weekday names (pattern with `EEE`).
      #
      # If not specified, the platform default will be used but for non-english platform default
      # an english parser will also be used as a fallback mechanism.
      config :locale, :validate => :string

      # The date formats allowed are anything allowed by Joda-Time (java time
      # library). You can see the docs for this format here:
      #
      # http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html[joda.time.format.DateTimeFormat]
      #
      # An array with field name first, and format patterns following, `[ field,
      # formats... ]`
      #
      # If your time field has multiple possible formats, you can do this:
      # [source,ruby]
      #     match => [ "logdate", "MMM dd YYY HH:mm:ss",
      #               "MMM  d YYY HH:mm:ss", "ISO8601" ]
      #
      # The above will match a syslog (rfc3164) or `iso8601` timestamp.
      #
      # There are a few special exceptions. The following format literals exist
      # to help you save time and ensure correctness of date parsing.
      #
      # * `ISO8601` - should parse any valid ISO8601 timestamp, such as
      #   `2011-04-19T03:44:01.103Z`
      # * `UNIX` - will parse *float or int* value expressing unix time in seconds since epoch like 1326149001.132 as well as 1326149001
      # * `UNIX_MS` - will parse **int** value expressing unix time in milliseconds since epoch like 1366125117000
      # * `TAI64N` - will parse tai64n time values
      #
      # For example, if you have a field `logdate`, with a value that looks like
      # `Aug 13 2010 00:03:44`, you would use this configuration:
      # [source,ruby]
      #     filter {
      #       date {
      #         match => [ "logdate", "MMM dd YYYY HH:mm:ss" ]
      #       }
      #     }
      #
      # If your field is nested in your structure, you can use the nested
      # syntax `[foo][bar]` to match its value. For more information, please refer to
      # <<logstash-config-field-references>>
      config :match, :validate => :array, :default => []

      # Store the matching timestamp into the given target field.  If not provided,
      # default to updating the `@timestamp` field of the event.
      config :target, :validate => :string, :default => "@timestamp"

      # Append values to the `tags` field when there has been no
      # successful match
      config :tag_on_failure, :validate => :array, :default => ["_dateparsefailure"]

      # LOGSTASH-34
      DATEPATTERNS = %w{ y d H m s S }

      def initialize(config = {})
        super
        @parsers = Hash.new { |h,k| h[k] = [] }
      end # def initialize

      def register
        if @match.length < 2
          error_msg = "The match setting should contains first a field name and at least one date format, current value is #{@match}"
          raise LogStash::ConfigurationError, i18n(error_msg)
        end

        replace_underscore(@locale) if @locale && @locale.include?("_")
        locale = @locale ? Locale.forLanguageTag(@locale) : nil

        @sprintf_timezone = @timezone && !@timezone.index("%{").nil?

        field    = @config["match"][0]
        patterns = @config["match"][1..-1]
        setup_parser_factory(field, locale, patterns)
      end

      def filter(event)
        @logger.debug? && @logger.debug("Date filter: received event", :type => event["type"])

        @parsers.each do |field, parsers|
          @logger.debug? && @logger.debug("Date filter looking for field",
                                          :type => event["type"], :field => field)
          next unless event.include?(field)
          value = event[field]

          begin
            epochmillis = nil
            success = false
            last_exception = RuntimeError.new "Unknown"

            parsers.each do |parser_config|
              parser_config[:parser].each do |parser|
                begin
                  if use_sprintf?
                    epochmillis = parser.call(value, event.sprintf(@timezone))
                  else
                    epochmillis = parser.call(value)
                  end
                  success = true
                  break # success
                rescue StandardError, java.lang.Exception => e
                  last_exception = e
                end
              end 
              break if success
            end

            raise last_exception unless success

            # Convert joda DateTime to a ruby Time
            event[@target] = LogStash::Timestamp.at(epochmillis / 1000, (epochmillis % 1000) * 1000)
            filter_matched(event)

            @logger.debug? && @logger.debug("Date parsing done", :value => value, :timestamp => event[@target])
          rescue StandardError, java.langException => e
            @logger.warn("Failed parsing date from field", :field => field,
                         :value => value, :exception => e.message,
                         :config_parsers => parsers.collect {|x| x[:format]}.join(','),
                         :config_locale => @locale ? @locale : "default="+java.util.Locale.getDefault().toString()
                        )
            # Tag this event if we can't parse it. We can use this later to
            # reparse+reindex logs if we improve the patterns given.
            @tag_on_failure.each do |tag|
              event["tags"] ||= []
              event["tags"] << tag unless event["tags"].include?(tag)
            end
          end
        end 
        return event
      end # def filter

      private

      def setup_parser_factory(field, locale, patterns)
        patterns.each do |pattern|
          parsers = []
          case pattern
          when "ISO8601"
            parsers << build_iso8601_parser
          when "UNIX" # unix epoch
            parsers << build_unix_parser
          when "UNIX_MS" # unix epoch in ms
            parsers << build_unixms_parser
          when "TAI64N" # TAI64 with nanoseconds, -10000 accounts for leap seconds
            parsers << build_tai64n_parser
          else
            begin
              parsers << build_general_parser(locale, pattern)
            rescue java.lang.Exception => e
              raise LogStash::ConfigurationError, i18n("#{e.message} for pattern '#{pattern}'")
            end
          end

          @logger.debug("Adding type with date config", :type => @type, :field => field, :format => pattern)
          @parsers[field] << { :parser => parsers.flatten, :format => pattern }
        end
      end

      def timezone?
        @timezone && !@sprintf_timezone
      end

      def timezone
        @timezone
      end

      def use_sprintf?
        @sprintf_timezone
      end

      def i18n(error)
        I18n.t("logstash.agent.configuration.invalid_plugin_register",
               :plugin => "filter", :type => "date",
               :error => error )
      end

      def replace_underscore(locale)
        @logger.warn("Date filter now use BCP47 format for locale, replacing underscore with dash")
        locale.gsub!('_','-')
      end

      def build_iso8601_parser
        iso8601_parser = (timezone? ? Iso8601Parser.new(timezone) : Iso8601Parser.new)
        iso8601_parser.setup
        iso8601_parser.parsers
      end

      def build_unix_parser
         lambda do |date|
          raise "Invalid UNIX epoch value '#{date}'" unless /^\d+(?:\.\d+)?$/ === date || date.is_a?(Numeric)
          (date.to_f * 1000).to_i
        end
      end

      def build_unixms_parser
        lambda do |date|
          raise "Invalid UNIX epoch value '#{date}'" unless /^\d+$/ === date || date.is_a?(Numeric)
          date.to_i
        end
      end

      def build_tai64n_parser
        lambda do |date|
          # Skip leading "@" if it is present (common in tai64n times)
          date = date[1..-1] if date[0, 1] == "@"
          return (date[1..15].hex * 1000 - 10000)+(date[16..23].hex/1000000)
        end
      end

      def build_general_parser(locale, pattern)
        general_parser = (timezone? ? GeneralParser.new(use_sprintf?, timezone) : GeneralParser.new(use_sprintf?))
        general_parser.setup(locale, pattern)
        general_parser.parsers
      end

    end
  end
end
