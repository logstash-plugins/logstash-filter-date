Gem::Specification.new do |s|

  s.name            = 'logstash-filter-date'
  s.version         = '0.1.0'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "The date filter is used for parsing dates from fields, and then using that date or timestamp as the logstash timestamp for the event."
  s.description     = "Convert arbitrary date format into Logstash timestamp"
  s.authors         = ["Elasticsearch"]
  s.email           = 'richard.pijnenburg@elasticsearch.com'
  s.homepage        = "http://logstash.net/"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)+::Dir.glob('vendor/*')

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "group" => "filter" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash', '>= 1.4.0', '< 2.0.0'
  s.add_runtime_dependency 'logstash-input-generator'
  s.add_runtime_dependency 'logstash-codec-json'
  s.add_runtime_dependency 'logstash-output-null'
end

