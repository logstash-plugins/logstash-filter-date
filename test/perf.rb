# encoding: utf-8

require "benchmark/ips"
require "logstash/codecs/base"
require "logstash/filters/date"
require "logstash/filter_delegator"
require_relative "../spec/fixtures/old_date_filter"

# `gradlew build` makes this
require "build/libs/logstash-filter-date-3.1.0.jar"

class S
  def initialize(input, patterns)
    @input = input
    @patterns = patterns
    @reports = []
  end

  def print
    columns = [ @input, @patterns.join(", ") ]

    @reports.each do |report|
      columns << report.body.gsub(/s -.*/, "")
    end

    columns << format("%.2f", @reports[1].ips / @reports[0].ips)

    puts "| " + columns.join(" | ")  + " |"
  end

  def add_report(report, *args)
    @reports << report
  end

  def method_missing(m, *args)
    nil
  end
end

def verify(jd, rd, *input)
  input.collect do |text|
    je = LogStash::Event.new("mytime" => text)
    re = LogStash::Event.new("mytime" => text)

    jd.multi_filter([je])
    rd.multi_filter([re])

    if (re.timestamp != je.timestamp)
      puts "❌FAILURE. Timestamp check failed."
      puts "Existing: #{re.timestamp}"
      puts "Proposed: #{je.timestamp}"
      false
    else
      true
    end
  end
end


def bench(input, patterns, extraconfig={})
  name = if input.is_a?(Array)
    "[#{input.first}, ... #{input.count} more ]"
  else
    input
  end
  suite = S.new(name, patterns)

  config = { "match" => [ "mytime", *patterns ] }.merge(extraconfig)
  puts "Benchmarking: "
  puts "  Input: #{input}"
  puts "  Config: #{config.inspect}"

  Benchmark.ips do |x|
    x.config(:time => 8, :warmup => 4, :suite => suite)

    jd = LogStash::Filters::Date.new(config)
    rd = LogStash::Filters::DateRuby.new(config)
    rd.register

    if verify(jd, rd, *input).all?
      puts "✓ Verification succeeded."
    else
      puts "❌FAILURE"
    end

    events = if input.is_a?(Array)
      input.collect { |text| LogStash::Event.new("mytime" => text) }
    else
      [ LogStash::Event.new("mytime" => input) ]
    end

    x.report("existing date") do |iterations|
      x = 0
      while x < iterations
        x += 1
        rd.multi_filter(events)
      end
    end

    x.report("proposed date") do |iterations|
      x = 0
      while x < iterations
        x += 1
        jd.multi_filter(events)
      end
    end
  end

  suite.print
end

puts <<HEADER
| input | patterns | old code | new code | ratio |
|-------|----------|----------|----------|-------|
HEADER

bench("2010-01-01T01:01:01Z", ["ISO8601"])
bench("2010-01-01T01:01:01Z", ["YYYY", "ISO8601"])
bench("2010-01-01T01:01:01Z", ["YYYY", "YYYY-DD", "ISO8601"])
bench(Time.now.to_i, ["UNIX"])
bench(Time.now.to_f, ["UNIX"])
bench((Time.now.to_f * 1000).to_i, ["UNIX_MS"])
bench("January  1 2015", ["MMMM  d YYYY", "MMM dd YYYY"])
bench("January 11 2015", ["MMMM  d YYYY", "MMM dd YYYY"])
bench("Jan  1 2015", ["MMM  d YYYY", "MMM dd YYYY"])
bench("Jan 11 2015", ["MMM  d YYYY", "MMM dd YYYY"])
bench(50.times.map { "Jan  1 2015" }, ["MMM  d YYYY", "MMM dd YYYY"])
