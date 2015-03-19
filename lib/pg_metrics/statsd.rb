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

      if options[:pgbouncer]
        metrics = PgMetrics::PgbouncerMetrics::fetch_pgbouncer_metrics(APPNAME, options[:conn])
      else
        regexp = options[:exclude] ? options[:exclude] : nil

        metrics = if options[:dbname]
                    PgMetrics::Metrics::fetch_database_metrics(APPNAME, options[:conn], options[:dbname],
                                                               options[:dbstats], regexp)
                  else
                    PgMetrics::Metrics::fetch_instance_metrics(APPNAME, options[:conn], regexp)
                  end
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
      default_stats = [PgMetrics::Metrics::Functions,
                       PgMetrics::Metrics::Locks,
                       PgMetrics::Metrics::TableSizes,
                       PgMetrics::Metrics::IndexSizes,
                       PgMetrics::Metrics::TableStatio,
                       PgMetrics::Metrics::TableStats,
                       PgMetrics::Metrics::IndexStatio,
                       PgMetrics::Metrics::IndexStats].to_set
      options = {
        host: "localhost",
        port: 8125,
        conn: "",
        scheme: %(#{Socket.gethostname}.postgresql),
        dbstats: Set.new
      }

      stats = { added: Set.new, default: default_stats }

      OptionParser.new do |opts|
        opts.on("-h", "--host STATSD_HOST", "StatsD host") { |v| options[:host] = v }
        opts.on("-p", "--port STATSD_PORT", "StatsD port") { |v| options[:port] = v.to_i }
        opts.on("-c", "--connection CONN", "PostgreSQL connection string") { |v| options[:conn] = v }
        opts.on("-d", "--dbname DBNAME", "PostgreSQL database name for database metrics") { |v| options[:dbname] = v }
        opts.on("-e", "--exclude REGEXP", "Exclude objects matching given regexp") { |v| options[:exclude] = ::Regexp.new(v) }
        opts.on("-s", "--scheme SCHEME", "Metric namespace") { |v| options[:scheme] = v }
        opts.on("--only", "Collect only specified stats") { |v| stats[:default] = Set.new }
        opts.on("--[no-]functions", "Collect database function stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::Functions, v) }
        opts.on("--[no-]locks", "Collect database lock stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::Locks, v) }
        opts.on("--[no-]table-sizes", "Collect database table size stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::TableSizes, v) }
        opts.on("--[no-]index-sizes", "Collect database index size stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::IndexSizes, v) }
        opts.on("--[no-]table-statio", "Collect database table statio stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::TableStatio, v) }
        opts.on("--[no-]table-stats", "Collect database table stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::TableStats, v) }
        opts.on("--[no-]index-statio", "Collect database index statio stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::IndexStatio, v) }
        opts.on("--[no-]index-stats", "Collect database index stats") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::IndexStats, v) }
        opts.on("--[no-]table-free-space", "Collect database table free space stats (requires pg_freespacemap)") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::TableFreeSpace, v) }
        opts.on("--[no-]index-ideal-sizes", "Collect database index ideal size estimates") { |v| stats = mutate_stats(stats, PgMetrics::Metrics::IndexIdealSizes, v) }
        opts.on("--pgbouncer", "Collect pgbouncer stats") { |v| options[:pgbouncer] = true }
        opts.on("--verbose") { |v| options[:verbose] = true }
        opts.on("--version") { |v| options[:version] = v }
      end.order!(args)

      options[:dbstats] = stats[:added].merge(stats[:default])
      options
    end

    def self.mutate_stats(stats, key, do_add)
      if do_add
        stats[:added].add(key)
      else
        stats[:default].delete(key)
      end
      stats
    end

  end
end
