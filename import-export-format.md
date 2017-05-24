Import / Export Format
======================

Export Format
-------------
When run in `export` mode, the import/export utility creates a directory tree representing the designated resource node and its child nodes:
* The directory structure represents the hierarchy of nodes beginning at the Fedora repository root down to the exported resource.
* The RDF nodes of the exported resource and its children are each represented by a file of serialized properties in the specified RDF language (e.g., _turtle_, the default).

If the export includes binaries, the binaries (i.e., non-RDF nodes) of the exported resource are represented by
  * A file with a .binary extension that contains the binary bitstream.
  * A directory containing a `fcr%3Ametadata` file with the serialized properties associated with the binary.
  
**Note:** The examples which follow assume the default RDF language (`turtle`) was used for the export.
 
**_Example: Resource Export with Binaries_**

For example, an export with binaries of a resource at http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e that contains one non-RDF resource ('content') would look like this:
```text
\rest
 \1f
  \ee
   \45
    \fd
     |1fee45fd-f506-446f-b9e9-f274c06a620e.ttl
     \1fee45fd-f506-446f-b9e9-f274c06a620e
      |content.binary
      \content
       |fcr%3Ametadata.ttl
```

In the example above ...

* `rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e.ttl` contains the RDF properties for the resource at `http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e`:
```text
@prefix premis:  <http://www.loc.gov/premis/rdf/v1#> .
@prefix test:  <info:fedora/test/> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix skos:  <http://www.w3.org/2004/02/skos/core#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix acl:  <http://www.w3.org/ns/auth/acl#> .
@prefix xsi:  <http://www.w3.org/2001/XMLSchema-instance> .
@prefix xmlns:  <http://www.w3.org/2000/xmlns/> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix fedora:  <http://fedora.info/definitions/v4/repository#> .
@prefix xml:  <http://www.w3.org/XML/1998/namespace> .
@prefix ebucore:  <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#> .
@prefix ldp:  <http://www.w3.org/ns/ldp#> .
@prefix dcterms:  <http://purl.org/dc/terms/> .
@prefix iana:  <http://www.iana.org/assignments/relation/> .
@prefix xs:  <http://www.w3.org/2001/XMLSchema> .
@prefix event:  <http://fedora.info/definitions/v4/event#> .
@prefix config:  <info:fedoraconfig/> .
@prefix prov:  <http://www.w3.org/ns/prov#> .
@prefix foaf:  <http://xmlns.com/foaf/0.1/> .
@prefix dc:  <http://purl.org/dc/elements/1.1/> .

<http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e>
        rdf:type               fedora:Container ;
        rdf:type               fedora:Resource ;
        fedora:lastModifiedBy  "bypassAdmin"^^<http://www.w3.org/2001/XMLSchema#string> ;
        fedora:createdBy       "bypassAdmin"^^<http://www.w3.org/2001/XMLSchema#string> ;
        fedora:created         "2017-05-24T12:39:13.731Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
        fedora:lastModified    "2017-05-24T12:40:50.307Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
        rdf:type               ldp:RDFSource ;
        rdf:type               ldp:Container ;
        fedora:writable        "true"^^<http://www.w3.org/2001/XMLSchema#boolean> ;
        fedora:hasParent       <http://localhost:8080/rest/> ;
        ldp:contains           <http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e/content> .
```

* `rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e/content.binary` contains the bitstream of the non-RDF `content` node.
* `rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e/content/fcr%3Ametadata.ttl` contains the RDF properties of the non-RDF `content` node:
```text
@prefix premis:  <http://www.loc.gov/premis/rdf/v1#> .
@prefix test:  <info:fedora/test/> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix skos:  <http://www.w3.org/2004/02/skos/core#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix acl:  <http://www.w3.org/ns/auth/acl#> .
@prefix xsi:  <http://www.w3.org/2001/XMLSchema-instance> .
@prefix xmlns:  <http://www.w3.org/2000/xmlns/> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix fedora:  <http://fedora.info/definitions/v4/repository#> .
@prefix xml:  <http://www.w3.org/XML/1998/namespace> .
@prefix ebucore:  <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#> .
@prefix ldp:  <http://www.w3.org/ns/ldp#> .
@prefix dcterms:  <http://purl.org/dc/terms/> .
@prefix iana:  <http://www.iana.org/assignments/relation/> .
@prefix xs:  <http://www.w3.org/2001/XMLSchema> .
@prefix event:  <http://fedora.info/definitions/v4/event#> .
@prefix config:  <info:fedoraconfig/> .
@prefix prov:  <http://www.w3.org/ns/prov#> .
@prefix foaf:  <http://xmlns.com/foaf/0.1/> .
@prefix dc:  <http://purl.org/dc/elements/1.1/> .

<http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e/content>
        rdf:type                 fedora:Binary ;
        rdf:type                 fedora:Resource ;
        fedora:lastModifiedBy    "bypassAdmin"^^<http://www.w3.org/2001/XMLSchema#string> ;
        premis:hasSize           "320933"^^<http://www.w3.org/2001/XMLSchema#long> ;
        ebucore:hasMimeType      "image/jpeg"^^<http://www.w3.org/2001/XMLSchema#string> ;
        fedora:createdBy         "bypassAdmin"^^<http://www.w3.org/2001/XMLSchema#string> ;
        fedora:created           "2017-05-24T12:40:50.326Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
        fedora:lastModified      "2017-05-24T12:40:50.326Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
        premis:hasMessageDigest  <urn:sha1:3cd65cb076571a89460bfa20265ad0f7d44db4b2> ;
        ebucore:filename         "1-1248161543llOC.jpg"^^<http://www.w3.org/2001/XMLSchema#string> ;
        rdf:type                 ldp:NonRDFSource ;
        fedora:writable          "true"^^<http://www.w3.org/2001/XMLSchema#boolean> ;
        iana:describedby         <http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e/content/fcr:metadata> ;
        fedora:hasParent         <http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e> ;
        fedora:hasFixityService  <http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e/content/fcr:fixity> .
```

**_Example: Resource Export without Binaries_**

If the same resource as above was exported without binaries, the export directory contents would be the same except for the omission of the `1fee45fd-f506-446f-b9e9-f274c06a620e` directory and its contents, namely, the `content.binary` file and the `content` sub-directory:
```text
\rest
 \1f
  \ee
   \45
    \fd
     |1fee45fd-f506-446f-b9e9-f274c06a620e.ttl
```

The contents of the `1fee45fd-f506-446f-b9e9-f274c06a620e.ttl` file would be the same as above.

**_Example: Repository Export with Binaries_**

If, instead, the entire repository had been exported (i.e., an export of `http://localhost:8080/rest`) (with binaries in this example), the export directory contents would be the same as in the first example above, except for the addition of a `rest.ttl` file (assuming the repository contains only the one resource in the examples above):
```text
|rest.ttl
\rest
 \1f
  \ee
   \45
    \fd
     |1fee45fd-f506-446f-b9e9-f274c06a620e.ttl
     \1fee45fd-f506-446f-b9e9-f274c06a620e
      |content.binary
      \content
       |fcr%3Ametadata.ttl
```

The `rest.ttl` file contains the RDF properties of the repository root node:
```text
@prefix premis:  <http://www.loc.gov/premis/rdf/v1#> .
@prefix test:  <info:fedora/test/> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix skos:  <http://www.w3.org/2004/02/skos/core#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix acl:  <http://www.w3.org/ns/auth/acl#> .
@prefix xsi:  <http://www.w3.org/2001/XMLSchema-instance> .
@prefix xmlns:  <http://www.w3.org/2000/xmlns/> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix fedora:  <http://fedora.info/definitions/v4/repository#> .
@prefix xml:  <http://www.w3.org/XML/1998/namespace> .
@prefix ebucore:  <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#> .
@prefix ldp:  <http://www.w3.org/ns/ldp#> .
@prefix dcterms:  <http://purl.org/dc/terms/> .
@prefix iana:  <http://www.iana.org/assignments/relation/> .
@prefix xs:  <http://www.w3.org/2001/XMLSchema> .
@prefix event:  <http://fedora.info/definitions/v4/event#> .
@prefix config:  <info:fedoraconfig/> .
@prefix prov:  <http://www.w3.org/ns/prov#> .
@prefix foaf:  <http://xmlns.com/foaf/0.1/> .
@prefix dc:  <http://purl.org/dc/elements/1.1/> .

<http://localhost:8080/rest/>
        fedora:lastModified            "2017-05-24T12:39:13.719Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> ;
        rdf:type                       ldp:RDFSource ;
        rdf:type                       ldp:Container ;
        rdf:type                       ldp:BasicContainer ;
        fedora:writable                "true"^^<http://www.w3.org/2001/XMLSchema#boolean> ;
        rdf:type                       fedora:RepositoryRoot ;
        rdf:type                       fedora:Resource ;
        rdf:type                       fedora:Container ;
        ldp:contains                   <http://localhost:8080/rest/1f/ee/45/fd/1fee45fd-f506-446f-b9e9-f274c06a620e> ;
        fedora:hasTransactionProvider  <http://localhost:8080/rest/fcr:tx> .
```

The other files in the export (`1fee45fd-f506-446f-b9e9-f274c06a620e.ttl`, `content.binary`, and `fcr%3Ametadata.ttl` would be the same as in the first example above.
