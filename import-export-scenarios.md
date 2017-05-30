Import / Export Scenarios
=========================

Scenario 1: Export Collection and Members using Inbound References for Import into Another Repository
-----------------------------------------------------------------------------------------------------
This scenario presents the use case of a "collection" resource and one or more "member" resources related to the "collection" resource via some predicate on each of the "member" resources.  In this scenario, the "collection" resource _does not_ have a predicate enumerating its "member" resources.

**_Exporting_**

When exporting the collection resource, provide the `--inbound` option and add the appropriate inbound predicate to the `--predicates` option.  For example, if the collection resource is `http://localhost:8080/rest/00/29/67/80/00296780-00a6-40c8-915a-57038e803ee9` and the member resources have the `info:fedora/fedora-system:def/relations-external#isMemberOfCollection` predicate:
```text
java -jar fcrepo-import-export.jar --mode export --resource http://localhost:8080/rest/00/29/67/80/00296780-00a6-40c8-915a-57038e803ee9 --inbound --predicates info:fedora/fedora-system:def/relations-external#isMemberOfCollection --dir /tmp/collection-export
```
(plus any additional predicates or other options desired, such as `--binaries`).

The command above will export the specified resource (the "collection") as well as all resources that have the `info:fedora/fedora-system:def/relations-external#isMemberOfCollection` predicate with the "collection" resource as the object.

**_Importing_**

To import the "collection" resource and its "member" resources (into another repository, for example), import the entire exported package.  For example:
```text
java -jar fcrepo-import-export.jar --mode import --resource http://localhost:8080/rest --dir /tmp/collection-export
```
