Fedora 4 Import/Export Utility
==============================
[![Build Status](https://travis-ci.com/fcrepo4-labs/fcrepo-import-export.svg?branch=master)](https://travis-ci.com/fcrepo4-labs/fcrepo-import-export)
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
$ java -jar fcrepo-import-export.jar --mode export --resource http://localhost:8080/rest --dir /tmp/test --binaries --acls
INFO 15:15:10.048 (ArgParser) Saved configuration to: /tmp/importexport.config
INFO 15:15:10.091 (Exporter) Running exporter...
```

Running the import/export utility with command-line arguments
-------------------------------------------------------------

```sh
$ java -jar target/fcrepo-import-export-<version>.jar --mode [import|export] [options]
```

To change the import-export logging level (default is INFO), set the `fcrepo.log.importexport` system property when running the command, e.g.: (Note, available logging levels are: TRACE, DEBUG, INFO, WARN, and ERROR.)

```sh
$ java -Dfcrepo.log.importexport=WARN -jar target/fcrepo-import-export-<version>.jar --mode [import|export] [options]
```

To provide a user and password for Fedora basic authentication, use the `-u` or `--user` argument, e.g.:
```sh
-u fedoraAdmin:secret3
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

For example, to export all the resources from a Fedora repository at `http://localhost:8080/rest/`, and put binaries and rdf in `/tmp/test`:

```sh
java -jar fcrepo-import-export.jar --mode export --resource http://localhost:8080/rest/ --dir /tmp/test --binaries --acls
```

To then load that data into an empty Fedora repository at the same URL, run the same command, but using `--mode import` instead of `--mode export`.

To enable the audit log, use the `-a` or `--auditLog`:

```sh
java -jar fcrepo-import-export.jar --mode export --resource http://localhost:8080/rest/ --dir /tmp/test --binaries --auditLog
```

You can also set the audit log directory with `-Dfcrepo.log.importexport.logdir=/some/directory`.

To export using a predicate other than `ldp:contains`, use the `-p` or `--predicates` option with a coma-separated list of predicates:

```sh
java -jar fcrepo-import-export.jar --mode export --resource http://localhost:8080/rest/ --dir /tmp/test --binaries --predicate http://pcdm.org/models#hasMember,http://www.w3.org/ns/ldp#contains
```

To map URIs when importing into a Fedora repository running at a different URI, use the `-M` or `--map` option
to translate the URIs.  For example, if you exported from `http://localhost:8984/rest/dev/` and are importing
into `http://example.org:8080/fedora/rest/`:

```sh
java -jar fcrepo-import-export.jar --mode import --resource http://example.org:8080/fedora/rest/ --dir /tmp/test --binaries --map http://localhost:8984/rest/dev/,http://example.org:8080/fedora/rest/
```

To retrieve inbound references (for example, when exporting a collection and you also want to export the members that link to the collection), use the `-i` or `--inbound` option.  When enabled, resources that are linked to or from with the specified predicates will be exported.

To retrieve external content binaries (binaries on other systems linked to with the `message/external-body` content type), use the `-x` or `--external` option.  When enabled, the external binaries will be retrieved and included in the export.  When disabled, they will not be retrieved, and only pointers to them will be exported.

If running against a version of fedora in which fedora:lastModified, fedora:lastModifiedBy, fedora:created and fedora:createdBy cannot be set, run the import in legacy mode.  *WARNING: the imported resources will have different values for these fields than the original resources!*

```sh
java -jar fcrepo-import-export.jar --mode import --resource http://example.org:8080/fedora/rest/ --dir /tmp/test --binaries --legacyMode
```


Running the import/export utility with BagIt support
------------------------------------------------------

The import-export-utility supports import and export of [BagIt](https://tools.ietf.org/html/rfc8493) bags 
and has BagIt specific command line arguments in order to support a number of use cases. In order to provide additional
support for custom metadata, bag profiles, and serialization, the [bagit-support](https://github.com/duraspace/bagit-support/) 
library is used for bagging operations.

### BagIt Profile

[BagIt Profiles](https://bagit-profiles.github.io/bagit-profiles-specification/) allow creators and consumers of Bags to
agree on optional components of the Bags they are exchanging. Each profile is defined using a json file which outlines 
the constraints according to the BagIt Profiles specification.

To enable a bag profile, use the `-g` or `--bag-profile` option. The import/export utility currently supports the 
following bag profiles:

* [default](https://raw.githubusercontent.com/duraspace/bagit-support/master/src/main/resources/profiles/default.json)
* [aptrust](https://raw.githubusercontent.com/duraspace/bagit-support/master/src/main/resources/profiles/aptrust.json)
* [metaarchive](https://raw.githubusercontent.com/duraspace/bagit-support/master/src/main/resources/profiles/metaarchive.json)
* [perseids](https://raw.githubusercontent.com/duraspace/bagit-support/master/src/main/resources/profiles/perseids.json)
* [beyondtherepository](https://raw.githubusercontent.com/duraspace/bagit-support/master/src/main/resources/profiles/beyondtherepository.json)

### BagIt Hash Algorithms

Hash algorithms can be specified using the `--bag-algorithms` option. The algorithms specified must be separated by a 
`,` and must be supported by the bag profile. If an algorithm is specified as required by the bag profile, it will 
automatically be included when exporting a bag.

### BagIt Metadata

User supplied metadata for tag files can be provided with a Yaml file specified by the `-G` or `--bag-config` option.

The configuration file specified must have a top-level key matching the name of the metadata file with sub keys for
each field you wish to manually supply. For example, setting metadata elements in the `bag-info.txt`:

```yaml
bag-info.txt:
  Source-Organization: org.fcrepo
  Organization-Address: https://github.com/fcrepo4-labs/fcrepo-import-export
```

**Note:** The import-export-utility will generate values for the `Bagging-Date`, `Payload-Oxum`, `Bag-Size`, and
`BagIt-Profile-Identifier` fields as part of the export process.

#### Profile Requirements

Depending on the BagIt Profile used, certain fields are required:

* default
   * bag-info.txt: `Source-Organization`
* aptrust
   * bag-info.txt: `Source-Organization`
   * aptrust-info.txt: `Title`, `Access`, `Storage-Option`
* metaarchive
   * bag-info.txt: `Source-Organization`, `Contact-Name`, `Contact-Phone`, `Contact-Email`, `External-Description`
* beyondtherepository
   * bag-info.txt: `Source-Organization`

### Serialization

The import-export-utility supports serialization as part of import and export. For both import and export the format 
used for serialization MUST be in a bag profile's `Accepted-Serialization`. If not, the process will fail with a list of
accepted formats.

#### Import

During import if the import-export-utility detects that a bag is a regular file it will attempt to deserialize the bag
based on the content type of the file.

#### Export

For export, if a bag profile allows serialization the format can be specified with `-s` or `--bag-serialization` along 
with the desired format. Currently, the following formats are supported:

#### Profile Requirements

Similar to the Bag Metadata, each BagIt Profile specifies if it allows serialization and what type of formats are 
accepted:

| Bag Profile | Serialization | Supported Formats |
| ----------- | ------------- | ----------------  |
| default     | Optional      | tar               |
| aptrust     | Optional      | tar               |
| beyondtherepository     | Optional      | tar, zip, gzip  |
| metaarchive     | Optional      | tar           |
| perseids     | Required      | tar, zip, gzip   |

### BagIt Examples

Note: All examples use a Fedora repository at `http://localhost:8080/rest/`

#### Export using the default bag profile as a tarball with user supplied metadata

Create `bagit-config.yml` with a `bag-info.txt` section for metadata:
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

Execute the import-export-utility:
```sh
java -jar fcrepo-import-export.jar --mode export --resource http://localhost:8080/rest --dir /tmp/example_bag --binaries --bag-profile default --bag-serialization tar --bag-config /tmp/bagit-config.yml --bag-algorithms sha1 -u fedoraAdmin:secret3
```

#### Export using the APTrust profile with user supplied metadata

Create `bagit-config-aptrust.yml` with `bag-info.txt` and `aptrust-info.txt` sections:
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
  Storage-Region: Standard
```

Execute the import-export-utility:
```sh
java -jar fcrepo-import-export.jar --mode export --resource http://localhost:8080/rest --dir /tmp/example_bag --binaries --bag-profile aptrust --bag-config /tmp/bagit-config.yml --bag-algorithms md5,sha256 -u fedoraAdmin:secret3
```

Additional tag files can be created by adding top-level keys in the user supplied Yaml file like the `aptrust-info.txt` added in the `bagit-config-aptrust.yml` example.

Running the import/export utility with a configuration file
-----------------------------------------------------------

```sh
$ java -jar target/fcrepo-import-export-<version>.jar -c /path/to/config/file
```

The easiest way to see an example of the configuration file is to run the utility with command-line arguments and inspect the configuration file created.

That configuration file is [Yaml](http://yaml.org) and allows for the following options:

* mode: [import|export] # which mode to operate in
* dir: Directory to import from/export to
* acls: [true|false] # whether to import/export acl resources
* binaries: [true|false] # whether to import/export binary resources
* overwriteTombstones: [true|false] # whether to replace tombstones of previously deleted resources
* external: [true|false] # whether to retrieve external content binaries when exporting
* inbound: [true|false] # whether to export inbound references when exporting
* map: Old and new base URIs, separated by comma, to map URIs when importing.
* versions: [true|false] # whether to export versions of resources and binaries.
* resource: The resource to export/import
* rdfLang: The RDF language to export into or import from

and will look something like the following:

```
mode: export
dir: /tmp/test
resource: http://localhost:8080/rest/1
```

Namespaces
----------
Currently, if the first use of a particular namespace occurs in RDF that is POSTed or PUT to the repository, regardless of any specific prefix binding supplied in the submitted graph, Fedora will instead bind the new namespace to a system-generated prefix in the form "ns00x".  While this behavior is not incorrect, it is inconvenient: prefixes generated during import will not match prefix bindings in the source repository. In order to avoid this behavior, follow the steps below:
- Create a repository.json file and namespaces.cnd file, as described in [Best Practices - RDF Namespaces](https://wiki.duraspace.org/display/FEDORA4x/Best+Practices+-+RDF+Namespaces).
- Start the destination repository for import with -Dfcrepo.modeshape.configuration=file:/path/to/repository.json, as described in [Application Configuration](https://wiki.duraspace.org/display/FEDORA4x/Application+Configuration).

Setting -Dfcrepo.modeshape.configuration=file:/path/to/repository.json does not work if deploying Fedora using the one-click jar.

Import/Export Format
--------------------
[Import/export data format](import-export-format.md)

Import/Export Scenarios
-----------------------
[Import/export scenarios](import-export-scenarios.md)

Maintainers
-----------

- [Esmé Cowles](https://github.com/escowles)
- [Nick Ruest](https://github.com/ruebot)
