/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.importexport.common;


import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

/**
 * @author awoods
 * @author escowles
 * @author mikeAtUVa
 * @since 2016-09-06
 */
public abstract class FcrepoConstants {

    public static final String BINARY_EXTENSION = ".binary";
    public static final String EXTERNAL_RESOURCE_EXTENSION = ".external";

    public static final String EBUCORE_NAMESPACE = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#";
    public static final Property HAS_MIME_TYPE = createProperty(EBUCORE_NAMESPACE + "hasMimeType");

    public static final String IANA_NAMESPACE = "http://www.iana.org/assignments/relation/";
    public static final Property DESCRIBEDBY = createProperty(IANA_NAMESPACE + "describedby");

    public static final String LDP_NAMESPACE = "http://www.w3.org/ns/ldp#";
    public static final Resource CONTAINER = createResource(LDP_NAMESPACE + "Container");
    public static final Property MEMBERSHIP_RESOURCE = createProperty(LDP_NAMESPACE + "membershipResource");
    public static final Property HAS_MEMBER_RELATION = createProperty(LDP_NAMESPACE + "hasMemberRelation");
    public static final Property NON_RDF_SOURCE = createProperty(LDP_NAMESPACE + "NonRDFSource");
    public static final Property RDF_SOURCE = createProperty(LDP_NAMESPACE + "RDFSource");
    public static final Property CONTAINS = createProperty(LDP_NAMESPACE + "contains");

    public static final String PREMIS_NAMESPACE = "http://www.loc.gov/premis/rdf/v1#";
    public static final Property HAS_SIZE = createProperty(PREMIS_NAMESPACE + "hasSize");
    public static final Property HAS_MESSAGE_DIGEST = createProperty(PREMIS_NAMESPACE + "hasMessageDigest");

    public static final String RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final Property RDF_TYPE = createProperty(RDF_NAMESPACE + "type");

    public static final String REPOSITORY_NAMESPACE = "http://fedora.info/definitions/v4/repository#";
    public static final Resource INBOUND_REFERENCES = createResource(REPOSITORY_NAMESPACE + "InboundReferences");
    public static final Resource PAIRTREE = createResource(REPOSITORY_NAMESPACE + "Pairtree");
    public static final Resource FEDORA_RESOURCE = createResource(REPOSITORY_NAMESPACE + "Resource");
    public static final Resource REPOSITORY_ROOT = createResource(REPOSITORY_NAMESPACE + "RepositoryRoot");
    public static final Resource VERSION_RESOURCE = createResource(REPOSITORY_NAMESPACE + "Version");

    public static final String BAG_INFO_FIELDNAME = "Bag-Info";

    public static final Property CREATED_DATE = createProperty(REPOSITORY_NAMESPACE + "created");
    public static final Property CREATED_BY = createProperty(REPOSITORY_NAMESPACE + "createdBy");
    public static final Property LAST_MODIFIED_DATE = createProperty(REPOSITORY_NAMESPACE + "lastModified");
    public static final Property LAST_MODIFIED_BY = createProperty(REPOSITORY_NAMESPACE + "lastModifiedBy");

    public static final String FCR_METADATA_PATH = "fcr:metadata";

    public static final String FCR_VERSIONS_PATH = "fcr:versions";
    public static final Property HAS_VERSION = createProperty(REPOSITORY_NAMESPACE + "hasVersion");
    public static final Property HAS_VERSIONS = createProperty(REPOSITORY_NAMESPACE + "hasVersions");
    public static final Property HAS_VERSION_LABEL = createProperty(REPOSITORY_NAMESPACE + "hasVersionLabel");
}
