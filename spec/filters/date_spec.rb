# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/date"

puts "Skipping date performance tests because this ruby is not jruby" if RUBY_ENGINE != "jruby"
RUBY_ENGINE == "jruby" and describe LogStash::Filters::Date do
  after do
    org.logstash.filters.parser.JodaParser.setDefaultClock(org.logstash.filters.parser.JodaParser.wallClock);
  end

  context "when giving an invalid match config" do
    let(:options) { { "match" => ["mydate"] } }
    it "raises a configuration error" do
      expect { described_class.new(options) }.to raise_error(LogStash::ConfigurationError)
    end
  end

  describe "parsing with ISO8601" do
    config <<-CONFIG
      filter {
        date {
          match => [ "mydate", "ISO8601" ]
          locale => "en"
          timezone => "UTC"
        }
      }
    CONFIG

    times = {
      "2001-01-01T00:00:00-0800"         => "2001-01-01T08:00:00.000Z",
      "1974-03-02T04:09:09-0800"         => "1974-03-02T12:09:09.000Z",
      "2010-05-03T08:18:18+00:00"        => "2010-05-03T08:18:18.000Z",
      "2004-07-04T12:27:27-00:00"        => "2004-07-04T12:27:27.000Z",
      "2001-09-05T16:36:36+0000"         => "2001-09-05T16:36:36.000Z",
      "2001-11-06T20:45:45-0000"         => "2001-11-06T20:45:45.000Z",
      "2001-12-07T23:54:54Z"             => "2001-12-07T23:54:54.000Z",

      # TODO: This test assumes PDT
      #"2001-01-01T00:00:00.123"          => "2001-01-01T08:00:00.123Z",

      "2010-05-03T08:18:18.123+00:00"    => "2010-05-03T08:18:18.123Z",
      "2004-07-04T12:27:27.123-04:00"    => "2004-07-04T16:27:27.123Z",
      "2001-09-05T16:36:36.123+0700"     => "2001-09-05T09:36:36.123Z",
      "2001-11-06T20:45:45.123-0000"     => "2001-11-06T20:45:45.123Z",
      "2001-12-07T23:54:54.123Z"         => "2001-12-07T23:54:54.123Z",
      "2001-12-07T23:54:54,123Z"         => "2001-12-07T23:54:54.123Z",

      #Almost ISO8601 support, with timezone

      "2001-11-06 20:45:45.123-0000"     => "2001-11-06T20:45:45.123Z",
      "2001-12-07 23:54:54.123Z"         => "2001-12-07T23:54:54.123Z",
      "2001-12-07 23:54:54,123Z"         => "2001-12-07T23:54:54.123Z",

      #Almost ISO8601 support, without timezone

      "2001-11-06 20:45:45.123"     => "2001-11-06T20:45:45.123Z",
      "2001-11-06 20:45:45,123"     => "2001-11-06T20:45:45.123Z",

    }

    times.each do |input, output|
      sample("mydate" => input) do
        begin
          insist { subject.get("mydate") } == input
          insist { subject.get("@timestamp").time } == Time.iso8601(output).utc
        rescue
          #require "pry"; binding.pry
          raise
        end
      end
    end # times.each
  end

  describe "parsing with java SimpleDateFormat syntax" do
    config <<-CONFIG
      filter {
        date {
          match => [ "mydate", "MMM dd HH:mm:ss Z" ]
          locale => "en"
        }
      }
    CONFIG

    now = Time.now
    year = now.year
    require 'java'

    times = {
      "Nov 24 01:29:01 -0800" => "#{year}-11-24T09:29:01.000Z",
    }
    times.each do |input, output|
      sample("mydate" => input) do
        insist { subject.get("mydate") } == input
        insist { subject.get("@timestamp").time } == Time.iso8601(output).utc
      end
    end # times.each
  end

  describe "parsing with UNIX" do
    config <<-CONFIG
      filter {
        date {
          match => [ "mydate", "UNIX" ]
          locale => "en"
        }
      }
    CONFIG

    times = {
      "0"          => "1970-01-01T00:00:00.000Z",
      "1000000000" => "2001-09-09T01:46:40.000Z",
      "1478207457" => "2016-11-03T21:10:57.000Z",

      # LOGSTASH-279 - sometimes the field is a number.
      0          => "1970-01-01T00:00:00.000Z",
      1000000000 => "2001-09-09T01:46:40.000Z",
      1478207457 => "2016-11-03T21:10:57.000Z"
    }
    times.each do |input, output|
      sample("mydate" => input) do
        insist { subject.get("mydate") } == input
        insist { subject.get("@timestamp").time } == Time.iso8601(output).utc
      end
    end # times.each

    #Invalid value should not be evaluated to zero (String#to_i madness)
    sample("mydate" => "%{bad_value}") do
      insist { subject.get("mydate") } == "%{bad_value}"
      insist { subject.get("@timestamp") } != Time.iso8601("1970-01-01T00:00:00.000Z").utc
    end
  end

  describe "parsing microsecond-precise times with UNIX (#213)" do
    config <<-CONFIG
      filter {
        date {
          match => [ "mydate", "UNIX" ]
          locale => "en"
        }
      }
    CONFIG

    sample("mydate" => "1350414944.123456") do
      # Joda time only supports milliseconds :\
      insist { subject.timestamp.time } == Time.iso8601("2012-10-16T12:15:44.123-07:00").utc
    end

    #Support float values
    sample("mydate" => 1350414944.123456) do
      insist { subject.get("mydate") } == 1350414944.123456
      insist { subject.get("@timestamp").time } == Time.iso8601("2012-10-16T12:15:44.123-07:00").utc
    end

    #Invalid value should not be evaluated to zero (String#to_i madness)
    sample("mydate" => "%{bad_value}") do
      insist { subject.get("mydate") } == "%{bad_value}"
      insist { subject.get("@timestamp") } != Time.iso8601("1970-01-01T00:00:00.000Z").utc
    end

    # Regression test
    # Support numeric values that come through the JSON parser. These numbers appear as BigDecimal 
    # instead of Float.
    sample(LogStash::Json.load('{ "mydate": 1350414944.123456 }')) do
      insist { subject.get("mydate") } == 1350414944.123456
      p subject.to_hash
      insist { subject.get("@timestamp").time } == Time.iso8601("2012-10-16T12:15:44.123-07:00").utc
    end
  end

  describe "parsing with UNIX_MS" do
    config <<-CONFIG
      filter {
        date {
          match => [ "mydate", "UNIX_MS" ]
          locale => "en"
        }
      }
    CONFIG

    times = {
      "0"          => "1970-01-01T00:00:00.000Z",
      "456"          => "1970-01-01T00:00:00.456Z",
      "1000000000123" => "2001-09-09T01:46:40.123Z",

      # LOGSTASH-279 - sometimes the field is a number.
      0          => "1970-01-01T00:00:00.000Z",
      456          => "1970-01-01T00:00:00.456Z",
      1000000000123 => "2001-09-09T01:46:40.123Z"
    }
    times.each do |input, output|
      sample("mydate" => input) do
        insist { subject.get("mydate") } == input
        insist { subject.get("@timestamp").time } == Time.iso8601(output)
      end
    end # times.each
  end

  describe "parsing with UNIX and UNIX_MS" do
    config <<-CONFIG
      filter {
        date {
          match => [ "mydate", "UNIX", "UNIX_MS" ]
          locale => "en"
        }
      }
    CONFIG

    times = {
      "0"          => "1970-01-01T00:00:00.000Z",
      "1000000000" => "2001-09-09T01:46:40.000Z",
      "1000000000123" => "2001-09-09T01:46:40.123Z",
      "1478207457" => "2016-11-03T21:10:57.000Z",
      "1478207457.456" => "2016-11-03T21:10:57.456Z",

      # LOGSTASH-279 - sometimes the field is a number.
      0          => "1970-01-01T00:00:00.000Z",
      1000000000 => "2001-09-09T01:46:40.000Z",
      1000000000123 => "2001-09-09T01:46:40.123Z",
      1478207457 => "2016-11-03T21:10:57.000Z",
      1478207457.456 => "2016-11-03T21:10:57.456Z",
    }
    times.each do |input, output|
      sample("mydate" => input) do
        insist { subject.get("mydate") } == input
        insist { subject.get("@timestamp").time } == Time.iso8601(output)
      end
    end # times.each
  end

  describe "failed parses should not cause a failure (LOGSTASH-641)" do
    config <<-'CONFIG'
      input {
        generator {
          lines => [
            '{ "mydate": "this will not parse" }',
            '{ }'
          ]
          codec => json
          type => foo
          count => 1
        }
      }
      filter {
        date {
          match => [ "mydate", "MMM  d HH:mm:ss", "MMM dd HH:mm:ss" ]
          locale => "en"
        }
      }
      output {
        null { }
      }
    CONFIG

    agent do
      # nothing to do, if this crashes it's an error..
    end
  end

  describe "TAI64N support" do
    config <<-'CONFIG'
      filter {
        date {
          match => [ "t",  TAI64N ]
          locale => "en"
        }
      }
    CONFIG

    # Try without leading "@"
    sample("t" => "4000000050d506482dbdf024") do
      insist { subject.timestamp.time } == Time.iso8601("2012-12-22T01:00:46.767Z").utc
    end

    # Should still parse successfully if it's a full tai64n time (with leading
    # '@')
    sample("t" => "@4000000050d506482dbdf024") do
      insist { subject.timestamp.time } == Time.iso8601("2012-12-22T01:00:46.767Z").utc
    end
  end

  describe "accept match config option with hash value (LOGSTASH-735)" do
    config <<-CONFIG
      filter {
        date {
          match => [ "mydate", "ISO8601" ]
          locale => "en"
        }
      }
    CONFIG

    time = "2001-09-09T01:46:40.000Z"

    sample("mydate" => time) do
      insist { subject.get("mydate") } == time
      insist { subject.get("@timestamp").time } == Time.iso8601(time).utc
    end
  end

  describe "support deep nested field access" do
    config <<-CONFIG
      filter {
        date {
          match => [ "[data][deep]", "ISO8601" ]
          locale => "en"
        }
      }
    CONFIG

    sample("data" => { "deep" => "2013-01-01T00:00:00.000Z" }) do
      insist { subject.get("@timestamp").time } == Time.iso8601("2013-01-01T00:00:00.000Z").utc
    end
  end

  describe "failing to parse should not throw an exception" do
    config <<-CONFIG
      filter {
        date {
          match => [ "thedate", "yyyy/MM/dd" ]
          locale => "en"
        }
      }
    CONFIG

    sample("thedate" => "2013/Apr/21") do
      insist { subject.get("@timestamp") } != "2013-04-21T00:00:00.000Z"
    end
  end

   describe "success to parse should apply on_success config(add_tag,add_field...)" do
    config <<-CONFIG
      filter {
        date {
          match => [ "thedate", "yyyy/MM/dd" ]
          add_tag => "tagged"
        }
      }
    CONFIG

    sample("thedate" => "2013/04/21") do
      insist { subject.get("@timestamp") } != "2013-04-21T00:00:00.000Z"
      insist { subject.get("tags") } == ["tagged"]
    end
  end

   describe "failing to parse should not apply on_success config(add_tag,add_field...)" do
    config <<-CONFIG
      filter {
        date {
          match => [ "thedate", "yyyy/MM/dd" ]
          add_tag => "tagged"
        }
      }
    CONFIG

    sample("thedate" => "2013/Apr/21") do
      insist { subject.get("@timestamp") } != "2013-04-21T00:00:00.000Z"
      reject { subject.get("tags") }.include? "tagged"
    end
  end

  describe "failing to parse should apply tag_on_failure" do
    config <<-CONFIG
      filter {
        date {
          match => [ "thedate", "yyyy/MM/dd" ]
          tag_on_failure => ["date_failed"]
        }
      }
    CONFIG

    sample("thedate" => "2013/Apr/21") do
      insist { subject.get("@timestamp") } != "2013-04-21T00:00:00.000Z"
      insist { subject.get("tags") }.include? "date_failed"
    end
  end

  describe "parsing with timezone parameter" do
    config <<-CONFIG
      filter {
        date {
          match => ["mydate", "yyyy MMM dd HH:mm:ss"]
          locale => "en"
          timezone => "America/Los_Angeles"
        }
      }
    CONFIG

    require 'java'
    times = {
      "2013 Nov 24 01:29:01" => "2013-11-24T09:29:01.000Z",
      "2013 Jun 24 01:29:01" => "2013-06-24T08:29:01.000Z",
    }
    times.each do |input, output|
      sample("mydate" => input) do
        insist { subject.get("mydate") } == input
        insist { subject.get("@timestamp").time } == Time.iso8601(output).utc
      end
    end # times.each
  end

  describe "parsing with timezone from event" do
    config <<-CONFIG
      filter {
        date {
          match => ["mydate", "yyyy MMM dd HH:mm:ss"]
          locale => "en"
          timezone => "%{mytz}"
        }
      }
    CONFIG

    require 'java'
    times = {
      "2013 Nov 24 01:29:01" => "2013-11-24T09:29:01.000Z",
      "2013 Jun 24 01:29:01" => "2013-06-24T08:29:01.000Z",
    }
    times.each do |input, output|
      sample("mydate" => input, "mytz" => "America/Los_Angeles") do
        insist { subject.get("mydate") } == input
        insist { subject.get("@timestamp").time } == Time.iso8601(output).utc
      end
    end # times.each
  end

  describe "don't fail on next years DST switchover in CET" do
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "yyyy MMM dd HH:mm:ss" ]
          locale => "en"
          timezone => "CET"
        }
      }
    CONFIG

    before(:each) do
      logstash_time = Time.utc(2016,03,29,23,59,50)
      allow(Time).to receive(:now).and_return(logstash_time)
    end

    sample "2016 Mar 26 02:00:37" do
      p :subject => subject
      insist { subject.get("tags") } != ["_dateparsefailure"]
      insist { subject.get("@timestamp").to_s } == "2016-03-26T01:00:37.000Z"
    end
  end

  context "Default year handling when parsing with timezone from event" do

    describe "LOGSTASH-34 - Default year should be this year" do
      config <<-CONFIG
        filter {
          date {
            match => [ "message", "EEE MMM dd HH:mm:ss" ]
            locale => "en"
            timezone => "%{mytz}"
          }
        }
      CONFIG

      sample("message" => "Sun Jun 02 20:38:03", "mytz" => "UTC") do
        insist { subject.get("@timestamp").year } == Time.now.year
      end
    end

    describe "fill last year if december events arrive in january" do
      config <<-CONFIG
        filter {
          date {
            match => [ "message", "MMM dd HH:mm:ss" ]
            locale => "en"
            timezone => "%{mytz}"
          }
        }
      CONFIG

      before(:each) do
        logstash_time = Time.utc(2014,1,1,00,30,50)
        allow(Time).to receive(:now).and_return(logstash_time)
        org.logstash.filters.parser.JodaParser.setDefaultClock { org.joda.time.DateTime.new(2014,1,1,00,30,50, org.joda.time.DateTimeZone::UTC ) }
      end

      sample("message" => "Dec 31 23:59:00", "mytz" => "UTC") do
        insist { subject.get("@timestamp").year } == 2013
      end
    end

    describe "fill next year if january events arrive in december" do
      config <<-CONFIG
        filter {
          date {
            match => [ "message", "MMM dd HH:mm:ss" ]
            locale => "en"
            timezone => "%{mytz}"
          }
        }
      CONFIG

      before(:each) do
        logstash_time = Time.utc(2013,12,31,23,59,50)
        allow(Time).to receive(:now).and_return(logstash_time)
        org.logstash.filters.parser.JodaParser.setDefaultClock { org.joda.time.DateTime.new(2013,12,31,23,59,50, org.joda.time.DateTimeZone::UTC ) }
      end

      sample( "message" => "Jan 01 01:00:00", "mytz" => "UTC") do
        insist { subject.get("@timestamp").year } == 2014
      end
    end

    describe "don't fail on next years DST switchover in CET", :skip => "This test tries to parse a time that doesn't exist. '02:00:37' is a time that doesn't exist because this DST switch goes from 01:59:59 to 03:00:00, skipping 2am entirely. I don't know how this spec ever passed..." do
      config <<-CONFIG
        filter {
          date {
            match => [ "message", "MMM dd HH:mm:ss" ]
            locale => "en"
            timezone => "CET"
          }
        }
      CONFIG

      before(:each) do
        logstash_time = Time.utc(2016,03,29,23,59,50)
        allow(Time).to receive(:now).and_return(logstash_time)
        org.logstash.filters.parser.JodaParser.setDefaultClock { org.joda.time.DateTime.new(2016,03,29,23,59,50, org.joda.time.DateTimeZone::UTC ) }
      end

      sample "Mar 26 02:00:37" do
        insist { subject.get("tags") } != ["_dateparsefailure"]
        insist { subject.get("@timestamp").to_s } == "2016-03-26T01:00:37.000Z"
      end
    end
  end

  describe "LOGSTASH-34 - Default year should be this year" do
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "EEE MMM dd HH:mm:ss" ]
          locale => "en"
        }
      }
    CONFIG

    sample "Sun Jun 02 20:38:03" do
      insist { subject.get("@timestamp").year } == Time.now.year
    end
  end

  describe "fill last year if december events arrive in january" do
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "MMM dd HH:mm:ss" ]
          locale => "en"
          timezone => "UTC"
        }
      }
    CONFIG

    before(:each) do
      logstash_time = Time.utc(2014,1,1,00,30,50)
      allow(Time).to receive(:now).and_return(logstash_time)
      org.logstash.filters.parser.JodaParser.setDefaultClock { org.joda.time.DateTime.new(2014,1,1,00,30,50, org.joda.time.DateTimeZone::UTC ) }
    end

    sample "Dec 31 23:59:00" do
      insist { subject.get("@timestamp").year } == 2013
    end
  end

  describe "fill next year if january events arrive in december" do
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "MMM dd HH:mm:ss" ]
          locale => "en"
          timezone => "UTC"
        }
      }
    CONFIG

    before(:each) do
      logstash_time = Time.utc(2013,12,31,23,59,50)
      allow(Time).to receive(:now).and_return(logstash_time)
      org.logstash.filters.parser.JodaParser.setDefaultClock { org.joda.time.DateTime.new(2013,12,31,15,59,50, org.joda.time.DateTimeZone::UTC ) }
    end

    sample "Jan 01 01:00:00" do
      insist { subject.get("@timestamp").year } == 2014
    end
  end

  describe "Supporting locale only" do
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "dd MMMM yyyy" ]
          locale => "fr"
          timezone => "UTC"
        }
      }
    CONFIG

    sample "14 juillet 1789" do
      insist { subject.get("@timestamp").time } == Time.iso8601("1789-07-14T00:00:00.000Z").utc
    end
  end

  describe "Supporting locale+country in BCP47" do
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "dd MMMM yyyy" ]
          locale => "fr-FR"
          timezone => "UTC"
        }
      }
    CONFIG

    sample "14 juillet 1789" do
      insist { subject.get("@timestamp").time } == Time.iso8601("1789-07-14T00:00:00.000Z").utc
    end
  end

  describe "Supporting locale+country in POSIX (internally replace '_' by '-')" do
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "dd MMMM yyyy" ]
          locale => "fr_FR"
          timezone => "UTC"
        }
      }
    CONFIG

    sample "14 juillet 1789" do
      insist { subject.get("@timestamp").time } == Time.iso8601("1789-07-14T00:00:00.000Z").utc
    end
  end

  describe "http dates" do

    config <<-'CONFIG'
      filter {
        date {
          match => [ "timestamp", "dd/MMM/yyyy:HH:mm:ss Z" ]
          locale => "en"
        }
      }
    CONFIG

    sample("timestamp" => "25/Mar/2013:20:33:56 +0000") do
      insist { subject.get("@timestamp").time } == Time.iso8601("2013-03-25T20:33:56.000Z")
    end
  end

  describe "Support fallback to english for non-english default locale" do
    #Override default locale with non-english
    config <<-CONFIG
      filter {
        date {
          match => [ "message", "dd MMMM yyyy" ]
          timezone => "UTC"
        }
      }
    CONFIG

    around do |example|
      default = java.util.Locale.getDefault
      java.util.Locale.setDefault(java.util.Locale.forLanguageTag('fr-FR'))
      example.run
      java.util.Locale.setDefault(default)
    end

    sample "01 September 2014" do
      insist { subject.get("@timestamp").time } == Time.iso8601("2014-09-01T00:00:00.000Z").utc
    end
  end

  context "Default year handling when parsing with english fallback parser" do

    around do |example|
      default = java.util.Locale.getDefault
      java.util.Locale.setDefault(java.util.Locale.forLanguageTag('fr-FR'))
      example.run
      java.util.Locale.setDefault(default)
    end

    puts "override locale"
    describe "LOGSTASH-34 - Default year should be this year" do
      config <<-CONFIG
        filter {
          date {
            match => [ "message", "EEE MMM dd HH:mm:ss" ]
            timezone => "UTC"
          }
        }
      CONFIG

      sample "Sun Jun 02 20:38:03" do
        insist { subject.get("@timestamp").year } == Time.now.year
      end
    end

    describe "fill last year if december events arrive in january" do
      config <<-CONFIG
        filter {
          date {
            match => [ "message", "MMM dd HH:mm:ss" ]
            timezone => "UTC"
          }
        }
      CONFIG

      before(:each) do
        logstash_time = Time.utc(2014,1,1,00,30,50)
        allow(Time).to receive(:now).and_return(logstash_time)
        org.logstash.filters.parser.JodaParser.setDefaultClock { org.joda.time.DateTime.new(2014,1,1,00,30,50, org.joda.time.DateTimeZone::UTC) }
      end

      sample "Dec 31 23:59:00" do
        insist { subject.get("@timestamp").year } == 2013
      end
    end

    describe "fill next year if january events arrive in december" do
      config <<-CONFIG
        filter {
          date {
            match => [ "message", "MMM dd HH:mm:ss" ]
            timezone => "UTC"
          }
        }
      CONFIG

      before(:each) do
        logstash_time = Time.utc(2013,12,31,23,59,50)
        allow(Time).to receive(:now).and_return(logstash_time)
        org.logstash.filters.parser.JodaParser.setDefaultClock { org.joda.time.DateTime.new(2013,12,31,23,59,50, org.joda.time.DateTimeZone::UTC) }
      end

      sample "Jan 01 01:00:00" do
        insist { subject.get("@timestamp").year } == 2014
      end
    end
  end

  describe "metric counters" do
    subject { described_class.new("match" => [ "message", "yyyy" ]) }

    context "when date parses a date correctly" do
      let(:event) { ::LogStash::Event.new("message" => "1999") }
      it "increases the matches counter" do
        expect(subject.metric).to receive(:increment).with(:matches)
        subject.filter(event)
      end
    end

    context "when date parses a date correctly" do
      let(:event) { ::LogStash::Event.new("message" => "not really a year") }
      it "increases the matches counter" do
        expect(subject.metric).to receive(:increment).with(:failures)
        subject.filter(event)
      end
    end
  end

  describe "cancelled events" do
    subject { described_class.new("match" => [ "message", "yyyy" ]) }

    context "single cancelled event" do
      let(:event) do
        e = ::LogStash::Event.new("message" => "1999")
        e.cancel
        e
      end

      it "ignores and return cancelled" do
        expect{subject.filter(event)}.to_not change{event.timestamp}
        result = subject.filter(event)
        expect(result.cancelled?).to be_truthy
      end
    end

    context "cancelled events list" do
      let(:uncancelled_year) { 2001 }

      let(:now_year) do
        ::LogStash::Event.new.timestamp.year
      end

      let(:events) do
        list = []
        e = ::LogStash::Event.new("message" => "1999")
        e.cancel
        list << e

        e = ::LogStash::Event.new("message" => "2000")
        e.cancel
        list << e

        e = ::LogStash::Event.new("message" => uncancelled_year.to_s)
        list << e

        list
      end

      it "ignores and return ignored cancelled" do
        result = subject.multi_filter(events)
        expect(result.size).to eq(3)
        expect(result[0].cancelled?).to be_truthy
        expect(result[1].cancelled?).to be_truthy
        expect(result[2].cancelled?).to be_falsey

        expect(result[0].timestamp.year).to eq(now_year)
        expect(result[1].timestamp.year).to eq(now_year)
        expect(result[2].timestamp.year).to eq(uncancelled_year)
      end
    end
  end
end
