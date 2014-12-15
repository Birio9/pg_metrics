require "helper"
require "set"

module PgMetrics
  module Test
    class Statsd < ::Test::Unit::TestCase
      def test_ok
        assert(true)
      end

      def test_should_have_sensible_defaults
        args = %w()
        config = PgMetrics::Statsd::parse(args)
        assert_equal("localhost", config[:host])
        assert_equal(8125, config[:port])
        assert_equal("", config[:conn])
        assert_match(/\.postgresql$/, config[:scheme])
      end

      def test_should_set_host_and_port
        args = %w(--host 127.0.0.1 --port 9000)
        config = PgMetrics::Statsd::parse(args)
        assert_equal("127.0.0.1", config[:host])
        assert_equal(9000, config[:port])
      end

      def test_should_set_regexp_filter
        args = %w(--exclude xdrop)
        config = PgMetrics::Statsd::parse(args)
        assert_equal(config[:exclude], ::Regexp.new(/xdrop/))
      end

      def test_should_set_connection
        args = ["--connection", "host=localhost port=5493"]
        config = PgMetrics::Statsd::parse(args)
        assert_equal(config[:conn], "host=localhost port=5493")
      end

      def test_should_set_dbname
        args = %w(--dbname prod)
        config = PgMetrics::Statsd::parse(args)
        assert_equal(config[:dbname], "prod")
      end

      def test_should_set_all_metrics
        args = []
        config = PgMetrics::Statsd::parse(args)
        expected = [PgMetrics::Metrics::Functions,
                    PgMetrics::Metrics::Locks,
                    PgMetrics::Metrics::TableSizes,
                    PgMetrics::Metrics::IndexSizes,
                    PgMetrics::Metrics::TableStatio,
                    PgMetrics::Metrics::TableStats,
                    PgMetrics::Metrics::IndexStatio,
                    PgMetrics::Metrics::IndexStats].to_set
        assert_equal(config[:dbstats], expected)
      end

      def test_should_set_all_metrics_with_positive_locks
        args = %w(--locks)
        config = PgMetrics::Statsd::parse(args)
        expected = [PgMetrics::Metrics::Functions,
                    PgMetrics::Metrics::Locks,
                    PgMetrics::Metrics::TableSizes,
                    PgMetrics::Metrics::IndexSizes,
                    PgMetrics::Metrics::TableStatio,
                    PgMetrics::Metrics::TableStats,
                    PgMetrics::Metrics::IndexStatio,
                    PgMetrics::Metrics::IndexStats].to_set
        assert_equal(config[:dbstats], expected)
      end

      def test_should_not_collect_locks
        args = %w(--no-locks)
        config = PgMetrics::Statsd::parse(args)
        expected = [PgMetrics::Metrics::Functions,
                    PgMetrics::Metrics::TableSizes,
                    PgMetrics::Metrics::IndexSizes,
                    PgMetrics::Metrics::TableStatio,
                    PgMetrics::Metrics::TableStats,
                    PgMetrics::Metrics::IndexStatio,
                    PgMetrics::Metrics::IndexStats].to_set
        assert_equal(config[:dbstats], expected)
      end

      def test_should_remove_all_but_locks
        args = %w(--no-functions --no-table-sizes --no-index-sizes --no-table-statio --no-table-stats --no-index-stats --no-index-statio)
        config = PgMetrics::Statsd::parse(args)
        expected = [PgMetrics::Metrics::Locks].to_set
        assert_equal(config[:dbstats], expected)
      end

    end
  end
end
