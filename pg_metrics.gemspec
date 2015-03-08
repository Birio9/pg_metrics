lib = File.expand_path("../lib/", __FILE__)
$LOAD_PATH.unshift lib unless $LOAD_PATH.include?(lib)
require "rake"
require "pg_metrics/version"

Gem::Specification.new do |spec|
  spec.name = "pg_metrics"
  spec.version = PgMetrics::VERSION
  spec.licenses = %w(MIT)
  spec.date = "2015-03-08"
  spec.summary = "pg_metrics"
  spec.description = "PostgreSQL Metrics"
  spec.authors = ["Michael Glaesemann"]
  spec.email = ["michael.glaesemann@meetme.com"]
  spec.files = FileList["{bin,lib,test}/**/*.*",
                        "CHANGELOG.markdown",
                        "DEV.markdown",
                        "Gemfile",
                        "Gemfile.lock",
                        "LICENSE",
                        "README.markdown",
                        "pg_metrics.gemspec",
                        "rakefile"].to_a
  spec.executables = %w(pg_metrics_statsd pg_metrics_sensu)
  spec.require_path = %(lib)
  spec.test_files = FileList["test/**/*.*"].to_a
  spec.extra_rdoc_files = %w(LICENSE README.markdown)
  spec.homepage = "http://rubygems.org/gems/pg_metrics"
  [["pg", ["~> 0.10"]],
   ["statsd-ruby", ["~> 1.2", ">= 1.2.1"]],
   ["sensu-plugin", ["~> 1.1"]]].each do |dep|
    spec.add_runtime_dependency(*dep)
  end

  [["test-unit", [["~> 2.1", ">= 2.1.2.0"]]],
   ["simplecov", ["~> 0.7", ">= 0.7.1"]]].each do |dep|
    spec.add_development_dependency(*dep)
  end
end
