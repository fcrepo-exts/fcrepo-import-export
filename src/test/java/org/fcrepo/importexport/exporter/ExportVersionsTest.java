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
package org.fcrepo.importexport.exporter;

import static java.util.Collections.emptyList;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.fcrepo.importexport.common.FcrepoConstants.CREATED_DATE;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_VERSIONS_PATH;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSION_LABEL;
import static org.fcrepo.importexport.common.FcrepoConstants.MEMENTO;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.REPOSITORY_ROOT;
import static org.fcrepo.importexport.common.FcrepoConstants.TIMEMAP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Resource;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.test.util.ResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;

/**
 * 
 * @author bbpennel
 *
 */
public class ExportVersionsTest {

    private final String BASE_URI = "http://localhost:8080/rest/";
    private String exportDirectory = "target/export-versions-test";
    private final String basedir = exportDirectory + "/versions";
    private URI rootResource;

    private final String rfc1123Date = "Wed, 13 Mar 2019 17:58:45 GMT";
    private final String versionCreated = "2019-03-13T17:58:45.110Z";
    private final String versionLabel = "20190313175845";

    @Mock
    private FcrepoClient client;
    @Mock
    private FcrepoClient.FcrepoClientBuilder clientBuilder;
    @Mock
    private FcrepoResponse headResponse;
    @Mock
    private HeadBuilder headBuilder;
    @Mock
    private Config config;
    @Mock
    private Logger auditLog;

    private ExporterWrapper exporter;

    private String[] predicates = new String[]{ CONTAINS.toString() };

    private List<URI> descriptionLinks =
            Arrays.asList(URI.create(RDF_SOURCE.getURI()));
    private List<URI> containerLinks =
            Arrays.asList(URI.create(CONTAINER.getURI()));
    private List<URI> binaryTypeLinks =
            Arrays.asList(URI.create(NON_RDF_SOURCE.getURI()));
    private List<URI> mementoTypeLinks =
            Arrays.asList(URI.create(RDF_SOURCE.getURI()),URI.create(MEMENTO.getURI()));
    private List<URI> timeMapTypeLinks =
            Arrays.asList(URI.create(RDF_SOURCE.getURI()),
                          URI.create(CONTAINER.getURI()),
                          URI.create(TIMEMAP.getURI()));
    private List<URI> binaryMementoLinks =
            Arrays.asList(URI.create(NON_RDF_SOURCE.getURI()), URI.create(MEMENTO.getURI()));

    @Before
    public void setUp() throws Exception {
        initMocks(this);

        when(clientBuilder.build()).thenReturn(client);

        final HeadBuilder headBuilder = mock(HeadBuilder.class);
        when(client.head(isA(URI.class))).thenReturn(headBuilder);
        when(headBuilder.disableRedirects()).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);
        when(headResponse.getStatusCode()).thenReturn(200);

        when(config.isExport()).thenReturn(true);
        when(config.includeVersions()).thenReturn(true);
        when(config.isIncludeBinaries()).thenReturn(true);
        when(config.getRdfLanguage()).thenReturn("application/ld+json");
        when(config.getRdfExtension()).thenReturn(".jsonld");
        when(config.getPredicates()).thenReturn(predicates);
        when(config.getBaseDirectory()).thenReturn(new File(basedir));
        when(config.getAuditLog()).thenReturn(auditLog);

        exporter = new ExporterWrapper(config, clientBuilder);

        rootResource = new URI("http://localhost:8080/rest");
        mockResponse(rootResource, containerLinks, new ArrayList<>(), null,
                createJson(rootResource, REPOSITORY_ROOT));
    }

    @After
    public void tearDown() {
        try {
            FileUtils.deleteDirectory(new File(exportDirectory));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mockResponse(final URI uri, final List<URI> typeLinks, final List<URI> describedbyLinks,
            final URI timemapLink, final String body) throws FcrepoOperationFailedException {
        ResponseMocker.mockHeadResponse(client, uri, typeLinks, describedbyLinks, timemapLink);

        ResponseMocker.mockGetResponse(client, uri, typeLinks, describedbyLinks, timemapLink, body);
    }

    @Test
    public void testExportVersionsOff() throws Exception {
        final URI resource1 = new URI(BASE_URI + "1");
        final URI resource1Versions = new URI(BASE_URI + "1/fcr:versions");
        final URI resource1Version1 = new URI(BASE_URI + "1/fcr:versions/version1");

        when(config.getResource()).thenReturn(resource1);
        when(config.includeVersions()).thenReturn(false);

        mockResponse(resource1, containerLinks, emptyList(), null, createJson(resource1));

        // Setup version responses, which should not get invoked
        final String versionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), resource1,
                resource1Version1, versionCreated, versionLabel));
        mockResponse(resource1Versions, emptyList(), emptyList(), null, versionsJson);
        mockResponse(resource1Version1, containerLinks, emptyList(), null, createJson(resource1));

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions.jsonld")));
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version1.jsonld")));
    }

    @Test
    public void testExportNonversionedResource() throws Exception {
        final URI resource1 = new URI(BASE_URI + "1");

        when(config.getResource()).thenReturn(resource1);

        mockResponse(resource1, containerLinks, emptyList(), null,  createJson(resource1));


        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertFalse("Versions directory should not be present for unversioned resource",
                exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions")));
    }

    @Test
    public void testExportVersionedContainer() throws Exception {
        final URI containerResource = new URI(BASE_URI + "container1");
        final URI containerResourceVersions = new URI(BASE_URI + "container1/fcr:versions");
        final URI containerResourceVersion = new URI(BASE_URI + "container1/fcr:versions/" + versionLabel);

        mockResponse(containerResource, containerLinks, emptyList(), containerResourceVersions,
                     createJson(containerResource));
        mockResponse(containerResourceVersion, mementoTypeLinks, Arrays.asList(containerResource),
                     containerResourceVersions, createJson(containerResource));

        final String versionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), containerResource,
            containerResourceVersion, versionCreated, versionLabel));
        mockResponse(containerResourceVersions, timeMapTypeLinks, Arrays.asList(containerResourceVersions),
                     containerResourceVersions, versionsJson);

        when(config.getResource()).thenReturn(containerResource);

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/container1.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/container1/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/container1/fcr%3Aversions/" + versionLabel +
                                               ".jsonld")));
    }

    @Test
    public void testExportVersionedBinary() throws Exception {
        final URI binaryResc = new URI(BASE_URI + "file1");
        final URI binaryRescMetadata = new URI(BASE_URI + "file1/fcr:metadata");
        final URI binaryRescVersions = new URI(BASE_URI + "file1/fcr:versions");
        final URI binaryRescMetadataVersions = new URI(BASE_URI + "file1/fcr:metadata/fcr:versions");
        final URI binaryRescVersion = new URI(BASE_URI + "file1/fcr:versions/" + versionLabel);
        final URI binaryRescMetadataVersion = new URI(BASE_URI + "file1/fcr:metadata/fcr:versions/" + versionLabel);
        mockResponse(binaryResc, binaryTypeLinks, Arrays.asList(binaryRescMetadata), binaryRescVersions, "binary");
        mockResponse(binaryRescVersion, binaryMementoLinks, Arrays.asList(binaryRescMetadata), binaryRescVersions,
               "old binary");

        mockResponse(binaryRescMetadata, descriptionLinks, Arrays.asList(binaryRescMetadata),
                binaryRescMetadataVersions, createJson(binaryRescMetadata));
        mockResponse(binaryRescMetadataVersion, mementoTypeLinks, Arrays.asList(binaryRescMetadataVersion),
                binaryRescMetadataVersions, createJson(binaryRescMetadataVersion));

        final String versionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), binaryResc,
                binaryRescVersion, versionCreated, versionLabel));
        mockResponse(binaryRescVersions, timeMapTypeLinks, Arrays.asList(binaryRescVersions), binaryRescVersions,
                     versionsJson);

        final String metadataVersionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), binaryRescMetadata,
                binaryRescMetadataVersion, versionCreated, versionLabel));
        mockResponse(binaryRescMetadataVersions,timeMapTypeLinks, Arrays.asList(binaryRescMetadataVersions),
                     binaryRescMetadataVersions, metadataVersionsJson);


        when(config.getResource()).thenReturn(binaryResc);

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1.binary")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Aversions/" + versionLabel +
                                               ".binary")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata/fcr%3Aversions/" +
                                               versionLabel + ".jsonld")));
    }

    @Test
    public void testExportBinaryFromRepositoryRoot() throws Exception {
        final URI binaryResc = new URI(BASE_URI + "file1");
        final URI binaryRescMetadata = new URI(BASE_URI + "file1/fcr:metadata");
        final URI binaryRescVersions = new URI(BASE_URI + "file1/fcr:versions");
        final URI binaryRescMetadataVersions = new URI(BASE_URI + "file1/fcr:metadata/fcr:versions");
        final URI binaryRescVersion = new URI(BASE_URI + "file1/fcr:versions/" + versionLabel);
        final URI binaryRescMetadataVersion = new URI(BASE_URI + "file1/fcr:metadata/fcr:versions/" + versionLabel);

        mockResponse(binaryResc, binaryTypeLinks, Arrays.asList(binaryRescMetadata), binaryRescVersions, "binary");
        mockResponse(binaryRescVersion, binaryMementoLinks, Arrays.asList(binaryRescMetadataVersion), null,
               "old binary");

        mockResponse(binaryRescMetadata, descriptionLinks, Arrays.asList(binaryRescMetadata),
                     binaryRescMetadataVersions,
                createJson(binaryRescMetadata));
        mockResponse(binaryRescMetadataVersion, mementoTypeLinks, Arrays.asList(binaryRescMetadataVersion), null,
                createJson(binaryRescMetadataVersion));

        final String versionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), binaryResc,
                binaryRescVersion, versionCreated, versionLabel));
        mockResponse(binaryRescVersions, timeMapTypeLinks, Arrays.asList(binaryRescVersions), null, versionsJson);

        final String metadataVersionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), binaryRescMetadata,
                binaryRescMetadataVersion, versionCreated, versionLabel));
        mockResponse(binaryRescMetadataVersions, timeMapTypeLinks, mementoTypeLinks, null, metadataVersionsJson);


        mockResponse(rootResource, containerLinks, new ArrayList<>(), null,
                createJson(rootResource, REPOSITORY_ROOT, binaryResc));
        final URI rootVersionsUri = URI.create(rootResource.toString() + "/" + FCR_VERSIONS_PATH);
        ResponseMocker.mockGetResponseError(client, rootVersionsUri, 500);

        when(config.getResource()).thenReturn(rootResource);

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1.binary")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata/fcr%3Aversions/" +
                   versionLabel + ".jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Aversions/" + versionLabel + ".binary")));
    }

    private String createJson(final URI resource, final URI... children) {
        return createJson(resource, null, children);
    }

    private String createJson(final URI resource, final Resource type, final URI... children) {
        final StringBuilder json = new StringBuilder("{\"@id\":\"" + resource.toString() + "\"");
        if (type != null) {
            json.append(",\"@type\":[\"" + type.getURI() + "\"]");
        }
        if (children != null && children.length > 0) {
            json.append(",\"" + CONTAINS.getURI() + "\":[")
                .append(Arrays.stream(children)
                    .map(child -> "{\"@id\":\"" + child.toString()  + "\"}")
                    .collect(Collectors.joining(",")))
                .append(']');
        }
        json.append('}');
        return json.toString();
    }

    private String joinJsonArray(final List<String> array) {
        return "[" + String.join(",", array) + "]";
    }

    private List<String> addVersionJson(final List<String> versions, final URI rescUri, final URI versionUri,
            final String label, final String timestamp) {
        final String versionJson = "{\"@id\":\"" + rescUri.toString() + "\"," +
                "\"" + CONTAINS.getURI() + "\":[{\"@id\":\"" + versionUri.toString() + "\"}]}," +
            "{\"@id\":\"" + versionUri.toString() + "\"," +
                "\"" + CREATED_DATE.getURI() + "\":[{" +
                    "\"@value\":\"" + timestamp + "\"," +
                    "\"@type\": \"http://www.w3.org/2001/XMLSchema#dateTime\"}]," +
                "\"" + HAS_VERSION_LABEL.getURI() + "\":[{" +
                    "\"@value\":\"" + label + " \"}]" +
            "}";
        versions.add(versionJson);
        return versions;
    }
}
