require 'optparse'
require 'socket'
require 'statsd-ruby'

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
                  PgMetrics::Metrics::fetch_database_metrics(APPNAME, options[:conn], options[:dbname], regexp)
                else
                  PgMetrics::Metrics::fetch_instance_metrics(APPNAME, options[:conn], regexp)
                end

      statsd = ::Statsd.new(options[:host], options[:port]).tap do |sd|
        sd.namespace = options[:scheme]
      end

      metrics.map! { |m| [m[0].join("."), m[1]] }

      if options[:verbose]
        STDOUT.puts %(#{metrics})
      end

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
        scheme: %(#{Socket.gethostname}.postgresql)
      }

      OptionParser.new do |opts|
        opts.on("-h", "--host STATSD_HOST", "StatsD host") { |v| options[:host] = v }
        opts.on("-p", "--port STATSD_PORT", "StatsD port") { |v| options[:port] = v.to_i }
        opts.on("-c", "--connection CONN", "PostgreSQL connection string") { |v| options[:conn] = v }
        opts.on("-d", "--dbname DBNAME", "PostgreSQL database name for database metrics") { |v| options[:dbname] = v }
        opts.on("-e", "--exclude REGEXP", "Exclude objects matching given regexp") { |v| options[:exclude] = ::Regexp.new(v) }
        opts.on("-s", "--scheme SCHEME", "Metric namespace") { |v| options[:scheme] = v }
        opts.on("--verbose") { |v| options[:verbose] = true }
        opts.on("--version") { |v| options[:version] = v }
      end.order!(args)

      options
    end
  end
end
