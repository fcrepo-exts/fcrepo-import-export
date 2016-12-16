Fedora 4 Import/Export Utility
==============================
[![Build Status](https://travis-ci.org/fcrepo4-labs/fcrepo-import-export.svg?branch=master)](https://travis-ci.org/fcrepo4-labs/fcrepo-import-export)
[![LICENSE](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](./LICENSE)

Work in progress
----------------

Open issues can be found [here](https://jira.duraspace.org/issues/?jql=project%20%3D%20FCREPO%20AND%20status%20in%20%28Open%2C%20%22In%20Progress%22%2C%20Reopened%2C%20%22In%20Review%22%2C%20Received%29%20AND%20component%20%3D%20f4-import-export).

Requirements:
* Java 8

Additional requirements for building:
* Maven 3

Building
--------

`mvn clean install`

Modes of execution
------------------
The standalone import/export utility can be run in either of two ways:

1. By passing in individual command-line arguments to the executable jar file
2. By passing in a single configuration file that contains the standard command-line arguments

The first time you run the utility with command-line arguments, a configuration file containing your provided arguments will be written to a file, the location of which will be displayed at the command line.

```sh
$ java -jar target/fcrepo-import-export-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest --dir /tmp/test --binaries
INFO 15:15:10.048 (ArgParser) Saved configuration to: /tmp/importexport.config
INFO 15:15:10.091 (Exporter) Running exporter...
...
```

Running the import/export utility with command-line arguments
-------------------------------------------------------------

```sh
$ java -jar target/fcrepo-import-export-<version>.jar --mode [import|export] [options]
```

To control the format of the exported RDF, the RDF language/serialization format can also be specified by adding, e.g.:

```sh
--rdfLang application/ld+json
```

The list of RDF languages supported:
- application/ld+json
- application/n-triples
- application/rdf+xml
- text/n3 (or text/rdf+n3)
- text/plain
- text/turtle (or application/x-turtle)    (**default**)

For example, to export all of the resources from a Fedora repository at `http://localhost:8080/rest/`, and put binaries and rdf in `/tmp/test`:

```sh
java -jar target/fcrepo-import-export-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest/ --dir /tmp/test --binaries
```

To then load that data into an empty Fedora repository at the same URL, run the same command, but using `--mode import` instead of `--mode export`.

To enable the audit log, use the `-a` or `--auditLog`:

```sh
java -jar target/fcrepo-import-export-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest/ --dir /tmp/test --binaries --auditLog
```

You can also set the audit log directory with `-Dfcrepo.log.importexport.logdir=/some/directory`.

To export using a predicate other than `ldp:contains`, use the `-p` or `--predicates` option with a coma-separated list of predicates:

```sh
java -jar target/fcrepo-import-export-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest/ --dir /tmp/test --binaries --predicate http://pcdm.org/models#hasMember,http://www.w3.org/ns/ldp#contains
```

Running the import/export utility with a BagIt support
------------------------------------------------------

You can export a [BagIt](https://tools.ietf.org/html/draft-kunze-bagit-14) bag from a Fedora repository based on a [BagIt Profile](https://github.com/ruebot/bagit-profiles/), and user supplied metadata for tag files can be provided with a Yaml file.

For example, to export all of the resources from a Fedora repository at `http://localhost:8080/rest/` in a BagIt bag using the default profile and user supplied metadata for tag files:

```sh
java -jar target/fcrepo-import-export-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest --dir /tmp/example_bag --binaries --bag-profile default --bag-config /tmp/bagit-config.yml
```

Where `bagit-config.yml` looks like:

```yaml
bag-info.txt:
  Source-Organization: York University Libraries
  Organization-Address: 4700 Keele Street Toronto, Ontario M3J 1P3 Canada
  Contact-Name: Nick Ruest
  Contact-Phone: +14167362100
  Contact-Email: ruestn@yorku.ca
  External-Description: Sample bag exported from fcrepo
  External-Identifier: SAMPLE_001
  Bag-Group-Identifier: SAMPLE
  Internal-Sender-Identifier: SAMPLE_001
  Internal-Sender-Description: Sample bag exported from fcrepo
```

For example, to export all of the resources from a Fedora repository at `http://localhost:8080/rest/` in a BagIt bag using the APTrust profile and user supplied metadata for tag files:

```sh
java -jar target/fcrepo-import-export-0.0.1-SNAPSHOT.jar --mode export --resource http://localhost:8080/rest --dir /tmp/example_bag --binaries --bag-profile aptrust --bag-config /tmp/bagit-config.yml
```

Where `bagit-config-aptrust.yml` looks like:
```yaml             
bag-info.txt:
  Source-Organization: York University Libraries
  Organization-Address: 4700 Keele Street Toronto, Ontario M3J 1P3 Canada
  Contact-Name: Nick Ruest
  Contact-Phone: +14167362100
  Contact-Email: ruestn@yorku.ca
  External-Description: Sample bag exported from fcrepo
  External-Identifier: SAMPLE_001
  Bag-Group-Identifier: SAMPLE
  Internal-Sender-Identifier: SAMPLE_001
  Internal-Sender-Description: Sample bag exported from fcrepo
aptrust-info.txt:
  Access: Restricted
  Title: Sample fcrepo bag
```

Additional tag files can be created by the utility by adding top-level classes in user supplied Yaml file like the `aptrust-info.txt` added in the `bagit-config-aptrust.yml` example.

Running the import/export utility with a configuration file
-----------------------------------------------------------

```sh
$ java -jar target/fcrepo-import-export-<version>.jar -c /path/to/config/file
```

The easiest way to see an example of the configuration file is to run the utility with command-line arguments and inspect the configuration file created.

That configuration file is [Yaml](http://yaml.org) and allows for the following options:

* mode: [import|export] # which mode to operate in
* dir: Directory to import from/export to
* binaries: [true|false] # whether is import/export binary resources
* source: Source hostname, used if the files were exported from a different hostname. Only for import mode.
* resource: The resource to export/import
* rdfLang: The RDF language to export into or import from

and will look something like the following:

```
mode: export
dir: /tmp/test
resource: http://localhost:8080/rest/1
```

Maintainers
-----------

- [Esmé Cowles](https://github.com/escowles)
- [Nick Ruest](https://github.com/ruebot)
