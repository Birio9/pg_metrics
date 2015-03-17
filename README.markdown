# pg_metrics PostgreSQL Metrics

`pg_metrics` is a PostgreSQL metrics collector for use with statsd.

## Installation

    gem install pg_metrics

The `pg_metrics_statsd` command is now available.


## Statsd

To collect PostgreSQL instance metrics on localhost port 5432 and pass them to a
statsd instance running on localhost port 8125:

    pg_metrics_statsd --host localhost --port 8125 --connection "host=localhost port=5432"

To collect PostgreSQL database metrics for the `prod` database, include the
`--dbname` parameter:

    pg_metrics_statsd --host localhost --port 8125 --connection "host=localhost port=5432" --dbname=prod

By default, pg_metrics_statsd collects stats from  `pg_locks`,
`pg_stat_user_functions` (where available), `pg_stat_user_tables`,
`pg_stat_user_tables`, `pg_stat_user_indexes`, `pg_statio_user_indexes`,
as well as per-table and per-index sizes. You can omit stats by supplying
command line flags:

 - `--no-functions`
 - `--no-locks`
 - `--no-table-stats`
 - `--no-table-statio`
 - `--no-index-stats`
 - `--no-index-statio`
 - `--no-table-sizes`
 - `--no-index-sizes`

### pgbouncer metrics

`pg_metrics` can also collect `pgbouncer` metrics by passing the `--pgbouncer`
flag.

    pg_metrics_statsd --host localhost --port 8125 --connection "host=localhost port=6432" --pgbouncer
