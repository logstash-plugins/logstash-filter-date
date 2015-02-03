# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/timestamp"

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
class LogStash::Filters::Date < LogStash::Filters::Base
  if RUBY_ENGINE == "jruby"
    JavaException = java.lang.Exception
    UTC = org.joda.time.DateTimeZone.forID("UTC")
  end

  config_name "date"
  milestone 3

  # Specify a time zone canonical ID to be used for date parsing.
  # The valid IDs are listed on the http://joda-time.sourceforge.net/timezones.html[Joda.org available time zones page].
  # This is useful in case the time zone cannot be extracted from the value,
  # and is not the platform default.
  # If this is not specified the platform default will be used.
  # Canonical ID is good as it takes care of daylight saving time for you
  # For example, `America/Los_Angeles` or `Europe/France` are valid IDs.
  config :timezone, :validate => :string

  # Specify a locale to be used for date parsing using either IETF-BCP47 or POSIX language tag.
  # Simple examples are `en`,`en-US` for BCP47 or `en_US` for POSIX.
  # If not specified, the platform default will be used.
  #
  # The locale is mostly necessary to be set for parsing month names (pattern with `MMM`) and
  # weekday names (pattern with `EEE`).
  #
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
  # http://www.elasticsearch.org/guide/en/logstash/current/_logstash_config_language.html#_field_references
  config :match, :validate => :array, :default => []

  # Store the matching timestamp into the given target field.  If not provided,
  # default to updating the `@timestamp` field of the event.
  config :target, :validate => :string, :default => "@timestamp"

  # Set to `true` to store the high-resolution timestamp data (micro- or nano-)
  # when matching using this filter. Regardless of whether this filter is enabled,
  # Joda will truncate the event's timestamp at millisecond resolution.
  #
  # This setting is currently only available with ISO-8601 date matching. When enabled, the
  # micro- and nano- second values (if available), will be saved to the field
  # "date_hires_value". The original timestamp will be saved to the field "date_hires_ts".
  # This should faciliate sorting by parsed_ts values when dealing with high-resolution time series.
  config :retain_high_precision, :validate => :boolean, :default => false

  # LOGSTASH-34
  DATEPATTERNS = %w{ y d H m s S }

  public
  def initialize(config = {})
    super

    @parsers = Hash.new { |h,k| h[k] = [] }
  end # def initialize

  public
  def register
    require "java"
    if @match.length < 2
      raise LogStash::ConfigurationError, I18n.t("logstash.agent.configuration.invalid_plugin_register",
        :plugin => "filter", :type => "date",
        :error => "The match setting should contains first a field name and at least one date format, current value is #{@match}")
    end

    locale = nil
    if @locale
      if @locale.include? '_'
        @logger.warn("Date filter now use BCP47 format for locale, replacing underscore with dash")
        @locale.gsub!('_','-')
      end
      locale = java.util.Locale.forLanguageTag(@locale)
    end
    setupMatcher(@config["match"].shift, locale, @config["match"] )
  end

  def setupMatcher(field, locale, value)
    value.each do |format|
      parsers = []
      case format
        when "ISO8601"
          iso_parser = org.joda.time.format.ISODateTimeFormat.dateTimeParser
          if @timezone
            iso_parser = iso_parser.withZone(org.joda.time.DateTimeZone.forID(@timezone))
          else
            iso_parser = iso_parser.withOffsetParsed
          end
          parsers << lambda { |date| iso_parser.parseMillis(date) }
          #Fall back solution of almost ISO8601 date-time
          almostISOparsers = [
            org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ").getParser(),
            org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser()
          ].to_java(org.joda.time.format.DateTimeParser)
          joda_parser = org.joda.time.format.DateTimeFormatterBuilder.new.append( nil, almostISOparsers ).toFormatter()
          if @timezone
            joda_parser = joda_parser.withZone(org.joda.time.DateTimeZone.forID(@timezone))
          else
            joda_parser = joda_parser.withOffsetParsed
          end
          parsers << lambda { |date| joda_parser.parseMillis(date) }
          @iso8601_format = true
        when "UNIX" # unix epoch
          parsers << lambda do |date|
            raise "Invalid UNIX epoch value '#{date}'" unless /^\d+(?:\.\d+)?$/ === date || date.is_a?(Numeric)
            (date.to_f * 1000).to_i
          end
        when "UNIX_MS" # unix epoch in ms
          parsers << lambda do |date|
            raise "Invalid UNIX epoch value '#{date}'" unless /^\d+$/ === date || date.is_a?(Numeric)
            date.to_i
          end
        when "TAI64N" # TAI64 with nanoseconds, -10000 accounts for leap seconds
          parsers << lambda do |date| 
            # Skip leading "@" if it is present (common in tai64n times)
            date = date[1..-1] if date[0, 1] == "@"
            return (date[1..15].hex * 1000 - 10000)+(date[16..23].hex/1000000)
          end
        else
          joda_parser = org.joda.time.format.DateTimeFormat.forPattern(format).withDefaultYear(Time.new.year)
          if @timezone
            joda_parser = joda_parser.withZone(org.joda.time.DateTimeZone.forID(@timezone))
          else
            joda_parser = joda_parser.withOffsetParsed
          end
          if (locale != nil)
            joda_parser = joda_parser.withLocale(locale)
          end
          parsers << lambda { |date| joda_parser.parseMillis(date) }
      end

      @logger.debug("Adding type with date config", :type => @type,
                    :field => field, :format => format)
      @parsers[field] << {
        :parser => parsers,
        :format => format
      }
    end
  end

  # def register

  public
  def filter(event)
    @logger.debug? && @logger.debug("Date filter: received event", :type => event["type"])
    return unless filter?(event)
    @parsers.each do |field, fieldparsers|
      @logger.debug? && @logger.debug("Date filter looking for field",
                                      :type => event["type"], :field => field)
      next unless event.include?(field)

      fieldvalues = event[field]
      fieldvalues = [fieldvalues] if !fieldvalues.is_a?(Array)
      fieldvalues.each do |value|
        next if value.nil?
        begin
          epochmillis = nil
          success = false
          last_exception = RuntimeError.new "Unknown"
          fieldparsers.each do |parserconfig|
            parserconfig[:parser].each do |parser|
              if @iso8601_format
                begin
                  # parse out the high resolution stuff
                  parsed_ts = /^(\d\d\d\d\-\d\d\-\d\dT\d\d:\d\d:\d\d)(?:(\.)(\d\d\d))*(\d{1,})*(.{1,})*$/.match(value)
                  @date_with_hires = value
                  value = parsed_ts[1] + parsed_ts[2] + parsed_ts[3] + parsed_ts[5]
                  if !parsed_ts[3].nil? || !parsed_ts[4].nil?
                    @high_precision = parsed_ts[3] + parsed_ts[4]
                  end
                rescue
                  # rescue me
                end
                if @high_precision
                  @logger.debug? && @logger.debug("parsed_ts timestamp data beyond joda resolution: ", :hires => parsed_ts)
                  if @retain_high_precision
                    event["date_hires_value"] = @high_precision
                    event["date_hires_ts"] = @date_with_hires
                  end
                end
              end

              begin
                epochmillis = parser.call(value)
                success = true
                break # success
              rescue StandardError, JavaException => e
                last_exception = e
              end
            end # parserconfig[:parser].each
            break if success
          end # fieldparsers.each

          raise last_exception unless success

          # Convert joda DateTime to a ruby Time
          event[@target] = LogStash::Timestamp.at(epochmillis / 1000, (epochmillis % 1000) * 1000)

          @logger.debug? && @logger.debug("Date parsing done", :value => value, :timestamp => event[@target])
          filter_matched(event)
        rescue StandardError, JavaException => e
          @logger.warn("Failed parsing date from field", :field => field,
                       :value => value, :exception => e)
          # Raising here will bubble all the way up and cause an exit.
          # TODO(sissel): Maybe we shouldn't raise?
          # TODO(sissel): What do we do on a failure? Tag it like grok does?
          #raise e
        end # begin
      end # fieldvalue.each
    end # @parsers.each

    return event
  end # def filter
end # class LogStash::Filters::Date
