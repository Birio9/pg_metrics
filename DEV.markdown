# Development notes

Processes I often forget between release cycles

## To build and install locally

    gem build pg_metrics.gemspec
    gem install ./pg_metrics-0.0.X.gem


## To release

* Update `CHANGELOG.markdown`
* Update `lib/pg_metrics/version.rb`
* Update `spec.date` in `pg_metrics.gemspec`
* Tag

        git tag -a "v0.0.X" -m "version 0.0.X"

* Push to github repo

        git push origin --tags
