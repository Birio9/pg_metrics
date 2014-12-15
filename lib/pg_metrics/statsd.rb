require 'optparse'
require 'socket'
require 'statsd-ruby'
require 'set'

module PgMetrics
  module Statsd
    APPNAME = "pg_metrics_statsd"

    def self.main(args)
      options = self.parse(args)

      if options[:version]
        STDOUT.puts %(pg_metrics #{PgMetrics::VERSION})
        return 0
      end

      regexp = options[:exclude] ? options[:exclude] : nil

      metrics = if options[:dbname]
                  PgMetrics::Metrics::fetch_database_metrics(APPNAME, options[:conn], options[:dbname],
                                                             options[:dbstats], regexp)
                else
                  PgMetrics::Metrics::fetch_instance_metrics(APPNAME, options[:conn], regexp)
                end

      statsd = ::Statsd.new(options[:host], options[:port]).tap do |sd|
        sd.namespace = options[:scheme]
      end

      metrics.map! { |m| [m[0].join("."), m[1]] }

      metrics.each { |m| STDOUT.puts m.join(" ") } if options[:verbose]

      metrics.each do |m|
        statsd.gauge(m[0], m[1])
      end

      exit 0
    end

    def self.parse(args)
      options = {
        host: "localhost",
        port: 8125,
        conn: "",
        scheme: %(#{Socket.gethostname}.postgresql),
        dbstats: [PgMetrics::Metrics::Functions,
                  PgMetrics::Metrics::Locks,
                  PgMetrics::Metrics::TableSizes,
                  PgMetrics::Metrics::IndexSizes,
                  PgMetrics::Metrics::TableStatio,
                  PgMetrics::Metrics::TableStats,
                  PgMetrics::Metrics::IndexStatio,
                  PgMetrics::Metrics::IndexStats].to_set
      }

      OptionParser.new do |opts|
        opts.on("-h", "--host STATSD_HOST", "StatsD host") { |v| options[:host] = v }
        opts.on("-p", "--port STATSD_PORT", "StatsD port") { |v| options[:port] = v.to_i }
        opts.on("-c", "--connection CONN", "PostgreSQL connection string") { |v| options[:conn] = v }
        opts.on("-d", "--dbname DBNAME", "PostgreSQL database name for database metrics") { |v| options[:dbname] = v }
        opts.on("-e", "--exclude REGEXP", "Exclude objects matching given regexp") { |v| options[:exclude] = ::Regexp.new(v) }
        opts.on("-s", "--scheme SCHEME", "Metric namespace") { |v| options[:scheme] = v }
        opts.on("--[no-]functions", "Collect database function stats") { |v| options[:dbstats].delete(PgMetrics::Metrics::Functions) unless v }
        opts.on("--[no-]locks", "Collect database lock stats") { |v| options[:dbstats].delete(PgMetrics::Metrics::Locks) unless v }
        opts.on("--[no-]table-sizes", "Collect database table size stats ") { |v| options[:dbstats].delete(PgMetrics::Metrics::TableSizes) unless v }
        opts.on("--[no-]index-sizes", "Collect database index size stats ") { |v| options[:dbstats].delete(PgMetrics::Metrics::IndexSizes) unless v }
        opts.on("--[no-]table-statio", "Collect database table statio stats ") { |v| options[:dbstats].delete(PgMetrics::Metrics::TableStatio) unless v }
        opts.on("--[no-]table-stats", "Collect database table stats ") { |v| options[:dbstats].delete(PgMetrics::Metrics::TableStats) unless v }
        opts.on("--[no-]index-statio", "Collect database index statio stats ") { |v| options[:dbstats].delete(PgMetrics::Metrics::IndexStatio) unless v }
        opts.on("--[no-]index-stats", "Collect database index stats ") { |v| options[:dbstats].delete(PgMetrics::Metrics::IndexStats) unless v }
        opts.on("--verbose") { |v| options[:verbose] = true }
        opts.on("--version") { |v| options[:version] = v }
      end.order!(args)

      options
    end
  end
end
