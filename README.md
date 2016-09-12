Fedora 4 Import/Export Utility
==============================

Work in progress
----------------


Requirements:
* Java 8

Additional requirements for building:
* Maven 3

Modes of execution
------------------
The standalone import/export utility can be run in either of two ways:
1) By passing in individual command-line arguments to the executable jar file
2) By passing in a single configuration file that contains the standard command-line arguments

The first time you run the utility with command-line arguments, a configuration file containing your provided arguments will be written to a file, the location of which will be displayed at the command line.

Running the import/export utility with command-line arguments
-------------------------------------------------------------

```sh
$ java -jar target/fcrepo-import-export-<version>.jar --mode [import|export] [options]
```

To control the format of the exported RDF, the extension and RDF language/serialization format can also be specified by adding, e.g.:

```sh
--rdfExt .jsonld --rdfLang application/ld+json
```

The list of RDF languages supported can be found in the [RESTful HTTP API documentation](application/ld+json).

For example, to export all of the resources from a Fedora repository at `http://localhost:8080/rest/`, and put binaries in `/tmp/bin` and RDF in `/tmp/rdf`:

```sh
java -jar target/fcrepo-import-export-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest/ --descDir /tmp/rdf --binDir /tmp/bin
```

To then load that data into an empty Fedora repository at the same URL, run the same command, but using `--mode import` instead of `--mode export`.

Running the import/export utility with a configuration file
-----------------------------------------------------------

```sh
$ java -jar target/fcrepo-import-export-<version>.jar -c /path/to/config/file
```

The easiest way to see an example of the configuration file is to run the utility with command-line arguments and inspect the configuration file created.
That configuration file will look something like the following:
> -m
> export
> -d
> /tmp/rdf
> -r
> http://localhost:8080/rest/1
