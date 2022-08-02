# DataLite

[![CI](https://github.com/philippkueng/datalite/actions/workflows/main.yml/badge.svg)](https://github.com/philippkueng/datalite/actions/workflows/main.yml)

**Disclaimer: This repo is highly experimental, expect APIs and implementations to change.**

Datalog meets [SQLite](https://www.sqlite.org/index.html) for low volume data.

## Reasoning & Vision

The thought being that while [Datomic](https://www.datomic.com/), [DataScript](https://github.com/tonsky/datascript), [Datalevin](https://github.com/juji-io/datalevin), [XTDB](https://xtdb.com/) all have their niche I was thinking of another niche, that it'd be handy to be able to query a SQLite database using Datalog and hence expand the reach of applications written but also tap in the tools that are being developed for SQLite currently eg. [litestream](https://litestream.io). 

This is the reason why I'd like to explore the idea of creating a wrapper on top of SQLite, mapping a schema as used for Datomic and create SQL tables from it supporting both nested maps and arrays (to a certain degree) and then offering the database to be queried with a [Datalog like language](https://docs.xtdb.com/language-reference/datalog-queries/) (from what I read this might not be a trivial feat). Furthermore, having a Clojure-wrapper over SQLite, with SQLite basically running anywhere from embedded devices to in the browser using WASM and Clojure being deployable pretty much anywhere too there's a chance that if one writes their local-first application against DataLite one can lift and shift it to another environment with minimal adaptations.

## License

Open Source but haven't decided on the actual license yet, recommendations welcome.