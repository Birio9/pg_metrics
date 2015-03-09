# pg_metrics Changelog

## Changes between 0.0.7

### Use client connection user in backend stats

In 0.0.5, backend-stats for automatic connections or non-force-user database
definitions used `SAMEUSER` as the username for the backend connection. The
actual client connection user is now used instead.

### Drop backend stats for pools with no corresponding backend

`pgbouncer` sometimes shows databases in `SHOW pools` that have no
corresponding entry in `SHOW databases`. These pool entries are
now ignored for backend stats.

## Changes between 0.0.5 and 0.0.6

### Add pgbouncer stats

`pg_metrics` can now be used to collect pgbouncer stats using the `--pgbouncer`
flag. Only results of `SHOW stats` and `SHOW pools` are collected, as well
as per-backend stats, which uses the results of `SHOW databases` to aggregate
pool stats per backend-connection, not just per defined database connection.

## Changes between 0.0.4 and 0.0.5

### Collect waiting session stats

Count sessions as *waiting* if `pg_stat_activity.waiting` is `TRUE`,
and as the session state otherwise (using `pg_stat_activity.state` for
PostgreSQL versions >= 9.2 and calculated from `pg_stat_activity.current_query`
in earlier versions).

`pg_stat_activity` does track waiting idependently of state, but for practical
purposes not all that useful to track them separately for metrics collection.

### Track xlog.location for PostgreSQL versions <= 9.0

Earlier versions of pg_metrics did not collect `current_xlog_location`
for versions earlier than 9.1. The `pg_current_xlog_location()` function
is available in both PostgreSQL 8.3 and 8.4. The `pg_last_xlog_(receive|replay)_location()`
functions are available for PostgreSQL versions >= 9.0, so collect those
when available.


## Changes between 0.0.3 and 0.0.4

### Allow specfication of which database stats are collected.

Prior to 0.0.4, per-database stats were collected only from `pg_locks`,
`pg_stat_user_tables` and `pg_statio_user_tables`. `pg_metrics_statsd`
collects all stats by default, and allows specification of which stats
to omit with a a variety of `--no-*` command line flags.

## Changes between 0.0.2 and 0.0.3

### Improve formatting of verbose output

0.0.3 prints each metric on its own line.

### Permit using short -s flag to specify scheme

0.0.3 allows you to specify scheme using `-s SCHEME` as well
as the legacy `--scheme SCHEME`.

## Changes between 0.0.1 and 0.0.2

### Fix use of regexp filter

A incomplete refactor left behind a second instantiation of the filter regex,
along with a reference to a variable that was no longer in scope.

## Set application_name only for PostgreSQL versions >= 9.0

The application_name parameter was introduced in PostgreSQL version 9.0. Earlier
versions (such as 8.3 and 8.4) will throw an error if you try to set it, so we
no longer try to set it for versions that don't support it.
