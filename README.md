Fedora 4 Import/Export Utility
==============================

Work in progress
----------------


Requirements:
* Java 8

Additional requirements for building and running from source:
* Maven 3


To run the standalone export utility:

```sh
$ java -jar fcrepo-import-export-driver/target/fcrepo-import-export-driver-<version>.jar --mode [import|export] [options]
```

To control the format of the exported RDF, the extension and RDF language/serialization format can also be specified by adding, e.g.:

```sh
--rdfExt .jsonld --rdfLang application/ld+json
```

The list of RDF languages supported can be found in the [RESTful HTTP API documentation](application/ld+json).

To run the utility with Maven:

```sh
$ mvn exec:java
```
