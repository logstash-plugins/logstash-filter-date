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
    java_import org.joda.time.LocalDateTime
    class LocalDateTime
      java_alias :to_datetime_with_tz, :toDateTime, [Java::org.joda.time.DateTimeZone]
    end
  end

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
  #
  # *More details on the syntax*
  #
  # The syntax used for parsing date and time text uses letters to indicate the
  # kind of time value (month, minute, etc), and a repetition of letters to
  # indicate the form of that value (2-digit month, full month name, etc).
  #
  # Here's what you can use to parse dates and times:
  #
  # [horizontal]
  # y:: year
  #   yyyy::: full year number. Example: `2015`.
  #   yy::: two-digit year. Example: `15` for the year 2015.
  #
  # M:: month of the year
  #   M::: minimal-digit month. Example: `1` for January and `12` for December.
  #   MM::: two-digit month. zero-padded if needed. Example: `01` for January  and `12` for December
  #   MMM::: abbreviated month text. Example: `Jan` for January. Note: The language used depends on your locale. See the `locale` setting for how to change the language.
  #   MMMM::: full month text, Example: `January`. Note: The language used depends on your locale.
  #
  # d:: day of the month
  #   d::: minimal-digit day. Example: `1` for the 1st of the month.
  #   dd::: two-digit day, zero-padded if needed. Example: `01` for the 1st of the month.
  #
  # H:: hour of the day (24-hour clock)
  #   H::: minimal-digit hour. Example: `0` for midnight.
  #   HH::: two-digit hour, zero-padded if needed. Example: `00` for midnight.
  #
  # m:: minutes of the hour (60 minutes per hour)
  #   m::: minimal-digit minutes. Example: `0`.
  #   mm::: two-digit minutes, zero-padded if needed. Example: `00`.
  #
  # s:: seconds of the minute (60 seconds per minute)
  #   s::: minimal-digit seconds. Example: `0`.
  #   ss::: two-digit seconds, zero-padded if needed. Example: `00`.
  #
  # S:: fraction of a second
  #   *Maximum precision is milliseconds (`SSS`). Beyond that, zeroes are appended.*
  #   S::: tenths of a second. Example:  `0` for a subsecond value `012`
  #   SS::: hundredths of a second. Example:  `01` for a subsecond value `01`
  #   SSS::: thousandths of a second. Example:  `012` for a subsecond value `012`
  #
  # Z:: time zone offset or identity
  #   Z::: Timezone offset structured as HHmm (hour and minutes offset from Zulu/UTC). Example: `-0700`.
  #   ZZ::: Timezone offset structured as HH:mm (colon in between hour and minute offsets). Example: `-07:00`.
  #   ZZZ::: Timezone identity. Example: `America/Los_Angeles`. Note: Valid IDs are listed on the http://joda-time.sourceforge.net/timezones.html[Joda.org available time zones page].
  #
  # z:: time zone names. *Time zone names ('z') cannot be parsed.*
  #
  # w:: week of the year
  #   w::: minimal-digit week. Example: `1`.
  #   ww::: two-digit week, zero-padded if needed. Example: `01`.
  #
  # D:: day of the year
  #
  # e:: day of the week (number)
  #
  # E:: day of the week (text)
  #   E, EE, EEE::: Abbreviated day of the week. Example:  `Mon`, `Tue`, `Wed`, `Thu`, `Fri`, `Sat`, `Sun`. Note: The actual language of this will depend on your locale.
  #   EEEE::: The full text day of the week. Example: `Monday`, `Tuesday`, ... Note: The actual language of this will depend on your locale.
  #
  # For non-formatting syntax, you'll need to put single-quote characters around the value. For example, if you were parsing ISO8601 time, "2015-01-01T01:12:23" that little "T" isn't a valid time format, and you want to say "literally, a T", your format would be this: "yyyy-MM-dd'T'HH:mm:ss"
  #
  # Other less common date units, such as era (G), century \(C), am/pm (a), and # more, can be learned about on the
  # http://www.joda.org/joda-time/key_format.html[joda-time documentation].
  config :match, :validate => :array, :default => []

  # Store the matching timestamp into the given target field.  If not provided,
  # default to updating the `@timestamp` field of the event.
  config :target, :validate => :string, :default => LogStash::Event::TIMESTAMP

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

    @sprintf_timezone = @timezone && !@timezone.index("%{").nil?

    setupMatcher(@config["match"].shift, locale, @config["match"] )
  end

  def parseWithJodaParser(joda_parser, date, format_has_year, format_has_timezone)
    return joda_parser.parseMillis(date) if format_has_year
    now = Time.now
    now_month = now.month
    if (format_has_timezone)
      result = joda_parser.parseDateTime(date)
    else
      # Parse date in UTC, Timezone correction later
      result = joda_parser.withZone(UTC).parseLocalDateTime(date)
    end

    event_month = result.getMonthOfYear

    if (event_month == now_month)
      result = result.with_year(now.year)
    elsif (event_month == 12 && now_month == 1)
      result = result.with_year(now.year-1)
    elsif (event_month == 1 && now_month == 12)
      result = result.with_year(now.year+1)
    else
      result = result.with_year(now.year)
    end

    if (format_has_timezone)
      return result.get_millis
    else
      #Timezone correction
      return result.to_datetime_with_tz(joda_parser.getZone()).get_millis
    end
  end

  def setupMatcher(field, locale, value)
    value.each do |format|
      parsers = []
      case format
        when "ISO8601"
          iso_parser = org.joda.time.format.ISODateTimeFormat.dateTimeParser
          if @timezone && !@sprintf_timezone
            iso_parser = iso_parser.withZone(org.joda.time.DateTimeZone.forID(@timezone))
          else
            iso_parser = iso_parser.withOffsetParsed
          end
          parsers << lambda { |date| iso_parser.parseMillis(date) }
          #Fall back solution of almost ISO8601 date-time
          almostISOparsers = [
            org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ").getParser(),
            org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
            org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSSZ").getParser(),
            org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS").getParser()
          ].to_java(org.joda.time.format.DateTimeParser)
          joda_parser = org.joda.time.format.DateTimeFormatterBuilder.new.append( nil, almostISOparsers ).toFormatter()
          if @timezone && !@sprintf_timezone
            joda_parser = joda_parser.withZone(org.joda.time.DateTimeZone.forID(@timezone))
          else
            joda_parser = joda_parser.withOffsetParsed
          end
          parsers << lambda { |date| joda_parser.parseMillis(date) }
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
          begin
            format_has_year = format.match(/y|Y/)
            format_has_timezone = format.match(/Z/)
            joda_parser = org.joda.time.format.DateTimeFormat.forPattern(format)
            if @timezone && !@sprintf_timezone
              joda_parser = joda_parser.withZone(org.joda.time.DateTimeZone.forID(@timezone))
            else
              joda_parser = joda_parser.withOffsetParsed
            end
            if locale
              joda_parser = joda_parser.withLocale(locale)
            end
            if @sprintf_timezone
              parsers << lambda { |date , tz|
                return parseWithJodaParser(joda_parser.withZone(org.joda.time.DateTimeZone.forID(tz)), date, format_has_year, format_has_timezone)
              }
            end
            parsers << lambda do |date|
              return parseWithJodaParser(joda_parser, date, format_has_year, format_has_timezone)
            end

            #Include a fallback parser to english when default locale is non-english
            if !locale &&
              "en" != java.util.Locale.getDefault().getLanguage() &&
              (format.include?("MMM") || format.include?("E"))
              en_joda_parser = joda_parser.withLocale(java.util.Locale.forLanguageTag('en-US'))
              parsers << lambda { |date| parseWithJodaParser(en_joda_parser, date, format_has_year, format_has_timezone) }
            end
          rescue JavaException => e
            raise LogStash::ConfigurationError, I18n.t("logstash.agent.configuration.invalid_plugin_register",
              :plugin => "filter", :type => "date",
              :error => "#{e.message} for pattern '#{format}'")
          end
      end

      @logger.debug("Adding type with date config", :type => @type,
                    :field => field, :format => format)
      @parsers[field] << {
        :parser => parsers,
        :format => format
      }
    end
  end

  def filter(event)
    @logger.debug? && @logger.debug("Date filter: received event", :type => event["type"])

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
              begin
                if @sprintf_timezone
                  epochmillis = parser.call(value, event.sprintf(@timezone))
                else
                  epochmillis = parser.call(value)
                end
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
                       :value => value, :exception => e.message,
                       :config_parsers => fieldparsers.collect {|x| x[:format]}.join(','),
                       :config_locale => @locale ? @locale : "default="+java.util.Locale.getDefault().toString()
                       )
          # Tag this event if we can't parse it. We can use this later to
          # reparse+reindex logs if we improve the patterns given.
          @tag_on_failure.each do |tag|
            event.tag(tag)
          end
        end
      end
    end

    return event
  end
end
