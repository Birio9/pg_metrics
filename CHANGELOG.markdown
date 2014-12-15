# pg_metrics Changelog

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

## Set application_name only for Postgres versions >= 9.0

The application_name parameter was introduced in PostgreSQL version 9.0. Earlier
versions (such as 8.3 and 8.4) will throw an error if you try to set it, so we
no longer try to set it for versions that don't support it.
