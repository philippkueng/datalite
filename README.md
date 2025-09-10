# DataLite

[![CI](https://github.com/philippkueng/datalite/actions/workflows/main.yml/badge.svg)](https://github.com/philippkueng/datalite/actions/workflows/main.yml)

**Disclaimer: This repo is highly experimental, expect APIs and implementations to change.**

Datalog meets [SQLite](https://www.sqlite.org/index.html), [DuckDB](https://duckdb.org/) or [PostgreSQL](https://www.postgresql.org/) for lower volume of data.

## Reasoning & Vision

The thought being that while [Datomic](https://www.datomic.com/), [DataScript](https://github.com/tonsky/datascript), [Datalevin](https://github.com/juji-io/datalevin), [XTDB](https://xtdb.com/) all have their niche I was thinking of another niche, that it'd be handy to be able to query a SQL database using Datalog and hence expand the reach of applications written but also tap into tools that are being developed for SQLite eg. [litestream](https://litestream.io) or DuckDB's capability to query CSVs. 

This is the reason why I'd like to explore the idea of creating a wrapper on top of SQL, mapping a schema as used for Datomic and create SQL tables from it supporting both nested maps and arrays (to a certain degree) and then offering the database to be queried with [Datalog](https://datomic.learn-some.com/).

Furthermore, having a Clojure-wrapper over SQLite, with SQLite running anywhere from embedded devices to in browser using WASM and Clojure being deployable pretty much anywhere too there's a chance that if one writes their local-first application against DataLite one can lift and shift it to another environment with minimal adaptations.

## Getting started

Add the dependency into your `deps.edn`

```clojure
io.github.philippkueng/datalite {:git/sha "bb271de51ed19bba48e90622f3f8b1d6b5be588c"}
```

Then head over to the [submit-tx-test](test/datalite/submit_tx_test.clj) which will demonstrate the how to apply the schema, submit data and query it.

## Run tests

```
clj -X:test
```

### Run a specific test namespace

```
clj -X:test :nses '[datalite.submit-tx-test]'
```

## License

Open Source but haven't decided on the actual license yet, recommendations welcome.
