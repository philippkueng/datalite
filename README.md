# DataLite

Datomic meets [SQLite](https://www.sqlite.org/index.html) for low volume data.

## Reasoning & Vision

The thought being that while [Datomic](https://www.datomic.com/), [DataScript](https://github.com/tonsky/datascript), [Datalevin](https://github.com/juji-io/datalevin), [XTDB](https://xtdb.com/), etc. are out there they're either too optimised for a particular environment or use-case, don't offer fulltext search or if they do they lack the integration in Database GUI applications for easy querying. 

This is the reason why I'd like to explore the idea of mapping a schema as used for Datomic and create SQL tables from it supporting both nested maps and arrays (to a certain degree) and then offering the database to be queried with a [Datalog like language](https://docs.xtdb.com/language-reference/datalog-queries/) (from what I read this might not be a trivial feat). Furthermore having a Clojure-wrapper over SQLite, with SQLite basically running anywhere from embedded devices to in the browser using WASM and Clojure being deployable pretty much anywhere too there's a chance that if one writes their local-first application against DataLite one can lift and shift it to another environment with minimal adaptations.

## License

Open Source but haven't decided on the actual license yet, recommendations welcome.