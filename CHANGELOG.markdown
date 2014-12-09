# pg_metrics Changelog

## Changes between 0.0.1 and 0.0.2

### Fix use of regexp filter

A incomplete refactor left behind a second instantiation of the filter regex,
along with a reference to a variable that was no longer in scope.

## Set application_name only for Postgres versions >= 9.0

The application_name parameter was introduced in PostgreSQL version 9.0. Earlier
versions (such as 8.3 and 8.4) will throw an error if you try to set it, so we
no longer try to set it for versions that don't support it.
