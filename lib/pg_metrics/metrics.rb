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
    TableFreeSpace = :table_free_space
    IndexIdealSizes = :index_ideal_size

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
          ? %{SELECT datname AS key, usename AS key2,
                     CASE WHEN waiting THEN 'waiting' ELSE state END AS key3,
                     count(*) AS value
              FROM pg_stat_activity
              WHERE pid <> pg_backend_pid() GROUP BY datname, usename, 3}
          : %{SELECT datname AS key, usename AS key2,
                CASE WHEN waiting THEN 'waiting'
                     ELSE CASE current_query
                               WHEN NULL THEN 'disabled'
                               WHEN '<IDLE>' THEN 'idle'
                               WHEN '<IDLE> in transaction' THEN 'idle in transaction'
                               ELSE 'active' END END AS key3,
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
          query: Gem::Version.new(server_version) >= Gem::Version.new('9.0') \
          ? %q{SELECT CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_xlog_location() END AS location,
                pg_last_xlog_receive_location() AS receive_location,
                pg_last_xlog_replay_location() AS replay_location}
          : %q{SELECT pg_current_xlog_location() AS location}
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
          WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')) AS funcs}
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
         AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')}
        },

        TableFreeSpace => {
          prefix: %w(table),
          query: Gem::Version.new(server_version) >= Gem::Version.new('8.4') \
          ? %{SELECT n.nspname AS key, t.relname AS key2,
                     COALESCE((SELECT sum(pg_freespace.avail) AS sum
                                 FROM pg_freespace(t.oid::regclass) AS pg_freespace(blkno, avail)), 0::bigint) AS free_space
                FROM pg_class t
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE t.relkind = 'r'::"char"
                      AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')}
          : %{SELECT n.nspname AS key, t.relname AS key2, fsm.bytes AS free_space
                FROM pg_class t
                JOIN pg_namespace n ON t.relnamespace = n.oid
                LEFT JOIN (SELECT fsm.relfilenode, sum(fsm.bytes) AS bytes
                             FROM pg_freespacemap_pages fsm
                             JOIN pg_database db ON db.oid = fsm.reldatabase
                                                    AND db.datname = current_database()
                             GROUP BY fsm.relfilenode) fsm ON t.relfilenode = fsm.relfilenode
                WHERE t.relkind = 'r'::"char"
                      AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')}
        },

        IndexSizes => {
          prefix: %w(table),
          query: %q{SELECT n.nspname AS key, cr.relname AS key2, 'index' AS key3,
              ci.relname AS key4, pg_relation_size(ci.oid) AS size
       FROM pg_class ci JOIN pg_index i ON ci.oid = i.indexrelid
       JOIN pg_class cr ON cr.oid = i.indrelid
       JOIN pg_namespace n on ci.relnamespace = n.oid
       WHERE ci.relkind = 'i' AND cr.relkind = 'r'
             AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')}
        },

        IndexIdealSizes => {
          prefix: %w(table),
          query: %{SELECT pg_namespace.nspname AS key, rel.relname AS key2,
       'index' AS key3, idx.relname AS key4,
       ((ceil(idx.reltuples * ((constants.index_tuple_header_size
                                  + constants.item_id_data_size
                                  + CASE WHEN (COALESCE(sum(CASE WHEN statts.staattnotnull THEN 0 ELSE 1 END), 0::bigint)
                                               + ((SELECT COALESCE(sum(CASE WHEN atts.attnotnull THEN 0 ELSE 1 END), 0::bigint) AS "coalesce"
                                                     FROM pg_attribute atts
                                                     JOIN (SELECT pg_index.indkey[the.i] AS attnum
                                                             FROM generate_series(0, pg_index.indnatts - 1) the(i)) cols ON atts.attnum = cols.attnum
                                                             WHERE atts.attrelid = pg_index.indrelid))) > 0
                                         THEN (SELECT the.null_bitmap_size
                                                        + constants.max_align
                                                        - CASE WHEN (the.null_bitmap_size % constants.max_align) = 0
                                                                 THEN constants.max_align
                                                               ELSE the.null_bitmap_size % constants.max_align END
                                                 FROM (VALUES (ceil(pg_index.indnatts::real / 8)::int)) the (null_bitmap_size))
                                         ELSE 0 END)::double precision
                                 + COALESCE(sum(statts.stawidth::double precision * (1::double precision - statts.stanullfrac)), 0::double precision)
                                 + COALESCE((SELECT sum(atts.stawidth::double precision * (1::double precision - atts.stanullfrac)) AS sum
                                               FROM pg_statistic atts
                                               JOIN (SELECT pg_index.indkey[the.i] AS attnum
                                                       FROM generate_series(0, pg_index.indnatts - 1) the(i)) cols ON atts.staattnum = cols.attnum
                                               WHERE atts.starelid = pg_index.indrelid), 0::double precision))
                            / (constants.block_size - constants.page_header_data_size::numeric - constants.special_space::numeric)::double precision)
           + constants.index_metadata_pages::double precision)
          * constants.block_size::double precision)::bigint AS ideal_size
  FROM pg_index
  JOIN pg_class idx ON pg_index.indexrelid = idx.oid
  JOIN pg_class rel ON pg_index.indrelid = rel.oid
  JOIN pg_namespace ON idx.relnamespace = pg_namespace.oid
  LEFT JOIN (SELECT pg_statistic.starelid, pg_statistic.staattnum, pg_statistic.stanullfrac, pg_statistic.stawidth, pg_attribute.attnotnull AS staattnotnull
               FROM pg_statistic
               JOIN pg_attribute ON (pg_statistic.starelid,pg_statistic.staattnum) = (pg_attribute.attrelid, pg_attribute.attnum)) statts
    ON statts.starelid = idx.oid
  CROSS JOIN (SELECT current_setting('block_size'::text)::numeric AS block_size,
                      CASE WHEN "substring"(version(), 12, 3) = ANY (ARRAY['8.0'::text, '8.1'::text, '8.2'::text]) THEN 27
                           ELSE 23 END AS tuple_header_size,
                      CASE WHEN version() ~ 'mingw32'::text THEN 8 ELSE 4 END AS max_align,
                      8 AS index_tuple_header_size,
                      4 AS item_id_data_size,
                      24 AS page_header_data_size,
                      0 AS special_space,
                      1 AS index_metadata_pages) AS constants
  WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
  GROUP BY pg_namespace.nspname, rel.relname, rel.oid, idx.relname, idx.reltuples, idx.relpages, pg_index.indexrelid,
           pg_index.indrelid, pg_index.indkey, pg_index.indnatts,
           constants.block_size, constants.tuple_header_size, constants.max_align,
           constants.index_tuple_header_size, constants.item_id_data_size, constants.page_header_data_size,
           constants.index_metadata_pages, constants.special_space;}
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
