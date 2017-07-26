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
package org.fcrepo.importexport.importer;

import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.FcrepoConstants.FEDORA_RESOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.junit.Test;

/**
 * 
 * @author bbpennel
 *
 */
public class VersionSubjectMappingStreamRDFTest {

    private final String RDF_LANG = "text/turtle";
    private final RDFFormat RDF_FORMAT = RDFFormat.TURTLE_PRETTY;

    private SubjectMappingStreamRDF mapper;

    private URI sourceUri;
    private URI destinationUri;

    @Test
    public void testRemapBase() throws Exception {
        sourceUri = URI.create("http://localhost:9999/rest");
        destinationUri = URI.create("http://example.org:8080/rest");

        final String rescUri = "http://localhost:9999/rest/con1";
        final String mappedRescUri = "http://example.org:8080/rest/con1";

        mapper = new VersionSubjectMappingStreamRDF(sourceUri, destinationUri);

        final Model model = makeModelWithResource(rescUri);

        final Model mappedModel = mapModel(model);
        final Resource mappedResc = mappedModel.listResourcesWithProperty(RDF_TYPE).next();

        assertEquals(mappedRescUri, mappedResc.getURI());
    }

    @Test
    public void testRemapBaseFromFile() throws Exception {
        final String mappedRescUri = "http://localhost:64199/fcrepo/rest/prod2";
        
        sourceUri = URI.create("http://localhost:8080/rest/dev/asdf");
        destinationUri = URI.create(mappedRescUri);
        
        File mFile = new File("src/test/resources/sample/mapped/rest/dev/asdf.ttl");
        mapper = new SubjectMappingStreamRDF(sourceUri, destinationUri);
        
        final Model mappedModel;
        try (final InputStream in2 = new FileInputStream(mFile)) {
            RDFDataMgr.parse(mapper, in2, contentTypeToLang(RDF_LANG));
        }
        mappedModel = mapper.getModel();
        
        final Resource mappedResc = mappedModel.listResourcesWithProperty(RDF_TYPE).next();
        
        assertEquals(mappedRescUri, mappedResc.getURI());
    }

    @Test
    public void testNoRemapping() throws Exception {
        final String rescUri = "http://localhost:8080/con1";

        mapper = new VersionSubjectMappingStreamRDF(sourceUri, destinationUri);

        final Model model = makeModelWithResource(rescUri);

        final Model mappedModel = mapModel(model);
        final Resource mappedResc = mappedModel.listResourcesWithProperty(RDF_TYPE).next();

        assertEquals(rescUri, mappedResc.getURI());
    }

    @Test
    public void testRemoveVersion() throws Exception {
        final String rescUri = "http://localhost:8080/con1";
        final String versionedUri = "http://localhost:8080/con1/fcr:versions/version_1";

        mapper = new VersionSubjectMappingStreamRDF(sourceUri, destinationUri);

        final Model model = makeModelWithResource(versionedUri);

        final Model mappedModel = mapModel(model);
        final Resource mappedResc = mappedModel.listResourcesWithProperty(RDF_TYPE).next();

        assertEquals(rescUri, mappedResc.getURI());
    }

    @Test
    public void testRemoveVersionChild() throws Exception {
        final String rescUri = "http://localhost:8080/con1/child";
        final String versionedUri = "http://localhost:8080/con1/fcr:versions/version_1/child";

        mapper = new VersionSubjectMappingStreamRDF(sourceUri, destinationUri);

        final Model model = makeModelWithResource(versionedUri);

        final Model mappedModel = mapModel(model);
        final Resource mappedResc = mappedModel.listResourcesWithProperty(RDF_TYPE).next();

        assertEquals(rescUri, mappedResc.getURI());
    }

    @Test
    public void testRemapBaseAndRemoveVersion() throws Exception {
        sourceUri = URI.create("http://localhost:9999/rest");
        destinationUri = URI.create("http://example.org:8080/rest");

        final String rescUri = "http://localhost:9999/rest/con1/fcr:versions/version_1";
        final String mappedRescUri = "http://example.org:8080/rest/con1";

        mapper = new VersionSubjectMappingStreamRDF(sourceUri, destinationUri);

        final Model model = makeModelWithResource(rescUri);

        final Model mappedModel = mapModel(model);
        final Resource mappedResc = mappedModel.listResourcesWithProperty(RDF_TYPE).next();

        assertEquals(mappedRescUri, mappedResc.getURI());
    }

    private Model makeModelWithResource(String uri) {
        final Model model = ModelFactory.createDefaultModel();
        final Resource resc = model.getResource(uri);
        resc.addProperty(RDF_TYPE, FEDORA_RESOURCE);

        return model;
    }

    private Model mapModel(Model model) throws Exception {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            RDFDataMgr.write(bos, model, RDF_FORMAT);
            try (final InputStream in2 = new ByteArrayInputStream(bos.toByteArray())) {
                RDFDataMgr.parse(mapper, in2, contentTypeToLang(RDF_LANG));
            }
        }
        return mapper.getModel();
    }
}
