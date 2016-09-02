Fedora 4 Import/Export Utility
==============================

Work in progress
----------------


Requirements:
* Java 8

Additional requirements for building and running from source:
* Maven 3


To run the standalone import/export utility:

```sh
$ java -jar fcrepo-import-export-driver/target/fcrepo-import-export-driver-<version>.jar --mode [import|export] [options]
```

To control the format of the exported RDF, the extension and RDF language/serialization format can also be specified by adding, e.g.:

```sh
--rdfExt .jsonld --rdfLang application/ld+json
```

The list of RDF languages supported can be found in the [RESTful HTTP API documentation](application/ld+json).

For example, to export all of the resources from a Fedora repository at `http://localhost:8080/rest/`, and put binaries in `/tmp/bin` and RDF in `/tmp/rdf`:

```sh
java -jar fcrepo-import-export-driver/target/fcrepo-import-export-driver-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest/ --descDir /tmp/rdf --binDir /tmp/bin
```

To then load that data into an empty Fedora repository at the same URL, run the same command, but using `--mode import` instead of `--mode export`.
