require "pg"
require "set"

module PgMetrics
  module Metrics

    Functions = :functions
    Locks = :locks
    TableSizes = :table_size
    IndexSizes = :index_size
    TableStatio = :table_statio
    TableStats = :table_stats
    IndexStatio = :index_statio
    IndexStats = :index_stats

    def self.fetch_instance_metrics(app_name, conn_info, regexp = nil)
      metrics = []
      conn = make_conn(conn_str(conn_info), app_name)
      server_version = conn.parameter_status("server_version")
      instance_metrics(server_version).values.each do |m|
        metrics += fetch_metrics(conn, m[:prefix], m[:query])
      end
      conn.finish
      filter_metrics(metrics, regexp)
    end

    def self.fetch_database_metrics(app_name, conn_info, dbname, select_names, regexp = nil)
      metrics = []
      conn = make_conn(conn_str(conn_info, dbname), app_name)
      server_version = conn.parameter_status("server_version")
      select_metrics = database_metrics(server_version).select { |k, _v| select_names.include? k }
      select_metrics.values.each do |m|
        metrics += fetch_metrics(conn, ["database", dbname] + m[:prefix], m[:query])
      end
      conn.finish
      filter_metrics(metrics, regexp)
    end

    def self.make_conn(conn_str, app_name)
      conn = PG::Connection.new(conn_str)
      server_version = conn.parameter_status("server_version")
      conn.exec(%(SET application_name = "#{app_name}")) if Gem::Version.new(server_version) >= Gem::Version.new("9.0")
      conn
    end

    def self.filter_metrics(metrics, regexp = nil)
      metrics.reject! { |m| m[1].nil? }
      metrics.reject! { |m| m[0].any? { |k| k =~ regexp  } } if regexp
      metrics
    end

    def self.conn_str(conn_info, dbname = "postgres")
      [conn_info, %(dbname=#{dbname})].join(" ")
    end

    def self.fetch_metrics(conn, keys, query)
      metrics = []

      return metrics if query.nil?

      timestamp = Time.now.to_i

      conn.exec(query) do |result|
        if result.nfields == 1 && result.ntuples == 1
          # Typically result of SHOW command
          metrics << format_metric(keys, result.getvalue(0, 0), timestamp)
        elsif result.nfields >= 2 && result.fields.first == "key"
          if result.fields.last == "value"
            # Omit "value" column from metric name
            nkeys = result.nfields - 1
            result.each_row do |row|
              mkeys = row.first(nkeys)
              value = row.last
              metrics << format_metric(keys + mkeys, value, timestamp)
            end
          else
            # Use any column named key* as part of the metric name.
            # Any other columns are named values.
            nkeys = result.fields.take_while { |f| f =~ /^key/ }.count
            keycols = result.fields.first(nkeys)
            nvals = result.nfields - nkeys
            valcols = result.fields.last(nvals)
            result.each do |tup|
              mkeys = keycols.map { |col| tup[col] }
              valcols.each do |key|
                value = tup[key]
                metrics << format_metric(keys + mkeys + [key], value, timestamp)
              end
            end
          end
        else # We've got a single-row result where columns are named values
          result[0].each do |key, value|
            metrics << format_metric(keys + [key], value, timestamp)
          end
        end
      end

      metrics
    end

    def self.format_metric(keys, value, timestamp)
      segs = keys.reject { |k| k.nil? }.map { |x| x.gsub(/[\s.]/, "_") }
      value = decode_xlog_location(value)
      [segs, value, timestamp]
    end

    def self.decode_xlog_location(val)
      return val if val.nil?
      if (m = val.match(%r{([A-Fa-f0-9]+)/([A-Fa-f0-9]+)}))
        return (m[1].hex << 32) + m[2].hex
      end
      val
    end

    def self.instance_metrics(server_version)
      {
        max_connections: {
          prefix: %w(config instance max_connections),
          query: %q{SHOW max_connections}
        },

        superuser_connections: {
          prefix: %w(config instance superuser_reserved_connections),
          query: %q{SHOW superuser_reserved_connections}
        },

        archive_files: {
          prefix: %w(archive_files),
          query: %q{SELECT CAST(COALESCE(SUM(CAST(archive_file ~ E'\\.ready$' AS int)), 0) AS INT) AS ready,
                         CAST(COALESCE(SUM(CAST(archive_file ~ E'\\.done$' AS int)), 0) AS INT) AS done
                    FROM pg_catalog.pg_ls_dir('pg_xlog/archive_status') AS archive_files (archive_file)}
        },

        bgwriter: {
          prefix: %w(bgwriter),
          query: %q{SELECT checkpoints_timed, checkpoints_req, buffers_checkpoint,
              buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc
       FROM pg_stat_bgwriter}
        },

        sessions: {
          prefix: %w(sessions),
          query: Gem::Version.new(server_version) >= Gem::Version.new('9.2') \
          ? %{SELECT datname AS key, usename AS key2, state AS key3, count(*) AS value
              FROM pg_stat_activity
              WHERE pid <> pg_backend_pid() GROUP BY datname, usename, state}
          : %{SELECT datname AS key, usename,
                   CASE current_query
                     WHEN NULL THEN 'disabled'
                     WHEN '<IDLE>' THEN 'idle'
                     WHEN '<IDLE> in transaction' THEN 'idle in transaction'
                     ELSE 'active' END,
               count(*) AS value
            FROM pg_stat_activity
            WHERE procpid <> pg_backend_pid() GROUP BY datname, usename, 3}
        },

        database_connection_limits: {
          prefix: %w(config database),
          query: %q{SELECT datname AS key,
              CASE WHEN datconnlimit <> -1 THEN datconnlimit ELSE current_setting('max_connections')::int END AS connection_limit
       FROM pg_database
       WHERE datallowconn AND NOT datistemplate}
        },

        user_connection_limits: {
          prefix: %w(config user),
          query: %q{SELECT rolname AS key,
              CASE WHEN rolconnlimit <> -1 THEN rolconnlimit ELSE current_setting('max_connections')::INT - CASE WHEN rolsuper THEN 0 ELSE current_setting('superuser_reserved_connections')::INT END END AS connection_limit
       FROM pg_roles
       WHERE rolcanlogin}
        },

        database_size: {
          prefix: %w(database),
          query: %q{SELECT datname AS key, pg_database_size(oid) AS size FROM pg_database WHERE NOT datistemplate}
        },

        streaming_state: {
          prefix: %w(streaming_state),
          query: Gem::Version.new(server_version) >= Gem::Version.new('9.1') \
          ? %q{SELECT CASE WHEN client_hostname IS NULL THEN 'socket' ELSE host(client_addr) END AS key,
                    CASE state WHEN 'catchup' THEN 1 WHEN 'streaming' THEN 2 ELSE 0 END as value
         FROM pg_stat_replication}
          : nil
        },

        transactions: {
          prefix: %w(database),
          query: %q{SELECT dat.datname AS key, 'transactions' AS key2, xact_commit AS commit, xact_rollback AS rollback FROM pg_stat_database JOIN pg_database dat ON dat.oid = datid WHERE datallowconn AND NOT datistemplate}
        },

        xlog: {
          prefix: %w(xlog),
          query: Gem::Version.new(server_version) >= Gem::Version.new('9.1') \
          ? %q{SELECT CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_xlog_location() END AS location,
                pg_last_xlog_receive_location() AS receive_location,
                pg_last_xlog_replay_location() AS replay_location}
          : nil
        }
      }
    end

    def self.database_metrics(server_version)
      {
        Functions => {
          prefix: %w(function),
          query: Gem::Version.new(server_version) >= Gem::Version.new('8.4') \
          ? %q{SELECT schemaname AS key,
array_to_string(ARRAY[funcname, '-', pronargs::TEXT,
                      CASE WHEN pronargs = 0 THEN ''
                      ELSE '-' || array_to_string(CASE WHEN pronargs > 16
                                                    THEN ARRAY(SELECT args[i]
                                                                 FROM generate_series(1, 8) AS _(i))
                                                          || '-'::TEXT
                                                          || ARRAY(SELECT args[i]
                                                                     FROM generate_series(pronargs - 7, pronargs) AS _ (i))
                                                          || funcid::TEXT
                                                    ELSE args END, '-') END], '') AS key2,
               calls, total_time, self_time
  FROM (SELECT funcid, schemaname, funcname::TEXT, pronargs,
               ARRAY(SELECT typname::TEXT
                       FROM pg_type
                       JOIN (SELECT args.i, proargtypes[args.i] AS typid
                               FROM pg_catalog.generate_series(0, array_upper(proargtypes, 1)) AS args (i))
                          AS args (i, typid) ON typid = pg_type.oid
                       ORDER BY i) AS args,
                      calls, total_time, self_time
          FROM pg_stat_user_functions
          JOIN pg_proc ON pg_proc.oid = funcid
          WHERE schemaname NOT IN ('information_schema', 'pg_catalog')) AS funcs}
          : nil
        },

        Locks => {
          prefix: %w(table),
          query: %q{SELECT nspname AS key,
                         CASE rel.relkind WHEN 'r' THEN rel.relname ELSE crel.relname END AS key2,
                         CASE rel.relkind WHEN 'r' THEN 'locks' ELSE 'index' END AS key3,
                         CASE rel.relkind WHEN 'r' THEN mode ELSE rel.relname END AS key4,
                         CASE rel.relkind WHEN 'r' THEN NULL ELSE 'locks' END AS key5,
                         CASE rel.relkind WHEN 'r' THEN NULL ELSE mode END AS key6,
                         count(*) AS value
  FROM pg_locks
  JOIN pg_database dat ON dat.oid = database
  JOIN pg_class rel ON rel.oid = relation
  LEFT JOIN pg_index ON indexrelid = rel.oid
  LEFT JOIN pg_class crel ON indrelid = crel.oid
  JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
  WHERE locktype = 'relation' AND nspname <> 'pg_catalog' AND rel.relkind in ('r', 'i')
  GROUP BY 1, 2, 3, 4, 5, 6}
        },

        TableSizes => {
          prefix: %w(table),
          query: %q{SELECT n.nspname AS key, r.relname AS key2,
              pg_relation_size(r.oid) AS size,
              pg_total_relation_size(r.oid) AS total_size
       FROM pg_class r
       JOIN pg_namespace n ON r.relnamespace = n.oid
       WHERE r.relkind = 'r'
         AND n.nspname NOT IN ('pg_catalog', 'information_schema')}
        },

        IndexSizes => {
          prefix: %w(table),
          query: %q{SELECT n.nspname AS key, cr.relname AS key2, 'index' AS key3,
              ci.relname AS key4, pg_relation_size(ci.oid) AS size
       FROM pg_class ci JOIN pg_index i ON ci.oid = i.indexrelid
       JOIN pg_class cr ON cr.oid = i.indrelid
       JOIN pg_namespace n on ci.relnamespace = n.oid
       WHERE ci.relkind = 'i' AND cr.relkind = 'r'
             AND n.nspname NOT IN ('pg_catalog', 'information_schema')}
        },

        TableStatio => {
          prefix: %w(table),
          query: %q{SELECT schemaname AS key, relname AS key2, 'statio' AS key3,
              nullif(heap_blks_read, 0) AS heap_blks_read,
              nullif(heap_blks_hit, 0) AS heap_blks_hit,
              nullif(idx_blks_read, 0) AS idx_blks_read,
              nullif(idx_blks_hit, 0) AS idx_blks_hit,
              nullif(toast_blks_read, 0) AS toast_blks_read,
              nullif(toast_blks_hit, 0) AS toast_blks_hit,
              nullif(tidx_blks_read, 0) AS tidx_blks_read,
              nullif(tidx_blks_hit, 0) AS tidx_blks_hit
       FROM pg_statio_user_tables}
        },

        TableStats => {
          prefix: %w(table),
          query: Gem::Version.new(server_version) >= Gem::Version.new('9.1') \
          ? %q{SELECT schemaname AS key, relname AS key2, 'stat' AS key3,
              nullif(seq_scan, 0) AS seq_scan,
              nullif(seq_tup_read, 0) AS seq_tup_read,
              nullif(idx_scan, 0) AS idx_scan,
              nullif(idx_tup_fetch, 0) AS idx_tup_fetch,
              nullif(n_tup_ins, 0) AS n_tup_ins,
              nullif(n_tup_upd, 0) AS n_tup_upd,
              nullif(n_tup_del, 0) AS n_tup_del,
              nullif(n_tup_hot_upd, 0) AS n_tup_hot_upd,
              nullif(n_live_tup, 0) AS n_live_tup,
              nullif(n_dead_tup, 0) AS n_dead_tup,
              nullif(vacuum_count, 0) AS vacuum_count,
              nullif(autovacuum_count, 0) AS autovacuum_count,
              nullif(analyze_count, 0) AS analyze_count,
              nullif(autoanalyze_count, 0) AS autoanalyze_count
       FROM pg_stat_user_tables} \
          : %q{SELECT schemaname AS key, relname AS key2, 'stat' AS key3,
              nullif(seq_scan, 0) AS seq_scan,
              nullif(seq_tup_read, 0) AS seq_tup_read,
              nullif(idx_scan, 0) AS idx_scan,
              nullif(idx_tup_fetch, 0) AS idx_tup_fetch,
              nullif(n_tup_ins, 0) AS n_tup_ins,
              nullif(n_tup_upd, 0) AS n_tup_upd,
              nullif(n_tup_del, 0) AS n_tup_del,
              nullif(n_tup_hot_upd, 0) AS n_tup_hot_upd,
              nullif(n_live_tup, 0) AS n_live_tup,
              nullif(n_dead_tup, 0) AS n_dead_tup
       FROM pg_stat_user_tables},
        },

        IndexStatio => {
          prefix: %w(table),
          query: %q{SELECT schemaname AS key, relname AS key2, 'index' AS key3,
                         indexrelname AS key4, 'statio' AS key5,
                          nullif(idx_blks_read, 0) AS idx_blks_read,
                          nullif(idx_blks_hit, 0) AS idx_blks_hit
                    FROM pg_statio_user_indexes},
        },

        IndexStats => {
          prefix: %w(table),
          query: %q{SELECT schemaname AS key, relname AS key2, 'index' AS key3,
                         indexrelname AS key4, 'stat' AS key5,
                         nullif(idx_scan, 0) AS idx_scan,
                         nullif(idx_tup_read, 0) AS idx_tup_read,
                         nullif(idx_tup_fetch, 0) AS idx_tup_fetch
         FROM pg_stat_user_indexes}
        }
      }
    end
  end
end
