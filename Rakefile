require 'logstash/devutils/rake'

task :vendor => :gradle

task :gradle => "gradle.properties" do
  sh("./gradlew clean vendor")
end

task "gradle.properties" do
  delete_create_gradle_properties
end

def delete_create_gradle_properties
  root_dir = File.dirname(__FILE__)
  gradle_properties_file = "#{root_dir}/gradle.properties"
  lsc_path = `bundle show logstash-core`
  FileUtils.rm_f(gradle_properties_file)
  File.open(gradle_properties_file, "w") do |f|
    f.puts "logstashCoreGemPath=#{lsc_path}"
  end
  puts "-------------------> Wrote #{gradle_properties_file}"
  puts `cat #{gradle_properties_file}`
end

task :test do
  require 'rspec'
  require 'rspec/core/runner'
  Rake::Task[:vendor].invoke
  sh './gradlew test'
  exit(RSpec::Core::Runner.run(Rake::FileList['spec/**/*_spec.rb']))
end
