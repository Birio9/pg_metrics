require "pg"

module PgMetrics
  module PgbouncerMetrics

    def self.fetch_pgbouncer_metrics(app_name, conn_info)
      metrics = []
      conn = make_conn(conn_str(conn_info), app_name)
      metrics = metrics.concat(fetch_stats_metrics(conn))
      pool_results = fetch_pools(conn)
      metrics = metrics.concat(extract_pool_metrics(pool_results))
      database_results = fetch_databases(conn)
      metrics = metrics.concat(extract_database_metrics(database_results))
      metrics = metrics.concat(extract_backend_metrics(database_results, pool_results))
      conn.finish
      filter_metrics(metrics)
    end

    def self.make_conn(conn_str, app_name)
      PG::Connection.new(conn_str)
    end

    def self.conn_str(conn_info, dbname = "pgbouncer", user = "admin")
      [conn_info, %(dbname=#{dbname}), %(user=#{user})].join(" ")
    end

    def self.filter_metrics(metrics, regexp = nil)
      metrics.reject! { |m| m[1].nil? }
      metrics.reject! { |m| m[0].any? { |k| k =~ regexp  } } if regexp
      metrics.inject([]) { |memo, m| memo << [sanitize_key(m[0]), m[1]] }
    end

    def self.fetch_stats_metrics(conn)
      cols = %w(total_requests total_received total_sent total_query_time avg_req avg_recv avg_sent avg_query)
      metrics = []
      conn.exec("SHOW stats") do |results|
        results.each do |tup|
          cols.each do |col|
            metrics << [["stats", tup["database"], col], tup[col]]
          end
        end
      end
      metrics
    end

    def self.fetch_pools(conn)
      conn.exec("SHOW pools")
    end

    def self.extract_pool_metrics(results)
      cols = %w(cl_active  cl_waiting sv_active sv_idle sv_used sv_tested sv_login maxwait)
      results.inject([]) do |memo, tup|
        cols.each do |col|
          memo << [["pools", tup["database"], tup["user"], col], tup[col]]
        end
        memo
      end
    end

    def self.fetch_databases(conn)
      conn.exec("SHOW databases")
    end

    def self.extract_database_metrics(results)
      cols = %w(pool_size reserve_pool)
      results.inject([]) do |memo, tup|
        cols.each do |col|
          memo << [["databases", tup["name"], col], tup[col]]
        end
        memo
      end
    end

    def self.extract_backend_metrics(database_results, pool_results)
      databases = database_results.inject({}) do |memo, tup|
        user = tup["force_user"].nil? ? :sameuser : tup["force_user"]
        host = tup["host"].nil? ? "localhost" : tup["host"]
        memo[tup["name"]] = {:host => host, :port => tup["port"], :database => tup["database"], :user => user}
        memo
      end

      sum_cols = %w(cl_active cl_waiting sv_active sv_idle sv_used sv_tested sv_login)
      max_cols = %w(max_wait)
      cols = sum_cols.concat(max_cols)
      sums = pool_results.inject({}) do |memo, tup|
        database = databases[tup["database"]]
        next memo if database.nil?
        user = database[:user] === :sameuser ? tup["user"] : database[:user]
        key = [database[:host], database[:port], database[:database], user]
        vals = memo[key] || {
          "cl_active" => 0,
          "cl_waiting" => 0,
          "sv_active" => 0,
          "sv_idle" => 0,
          "sv_used" => 0,
          "sv_tested" => 0,
          "sv_login" => 0,
          "max_wait" => 0
        }
        sum_cols.each { |col| vals[col] += tup[col].to_i }
        max_cols.each { |col| vals[col] = [tup[col].to_i, vals[col]].max }
        memo[key] = vals
        memo
      end
      sums.inject([]) do |memo, (key, val)|
        cols.each do |col|
          k = ["backends"] + key + [col]
          memo << [k, val[col]]
        end
        memo
      end
    end

    def self.sanitize_key(key)
      key.inject([]) { |memo, el| memo << el.gsub(/[^-a-zA-Z_0-9]/, "_") }
    end

  end
end
