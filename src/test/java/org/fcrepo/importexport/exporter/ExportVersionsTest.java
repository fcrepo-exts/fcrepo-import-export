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
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSION;
import static org.fcrepo.importexport.common.FcrepoConstants.HAS_VERSION_LABEL;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_SOURCE;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
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
    private String exportDirectory = "target/export";
    private final String basedir = exportDirectory + "/versions";

    private final String versionCreated = "2017-05-16T17:35:26.608Z";
    private final String versionLabel = "version1";

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
    private List<URI> containerLinks =
            Arrays.asList(URI.create(CONTAINER.getURI()));
    private List<URI> binaryLinks =
            Arrays.asList(URI.create(NON_RDF_SOURCE.getURI()));

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
            final String body) throws FcrepoOperationFailedException {
        ResponseMocker.mockHeadResponse(client, uri, typeLinks, describedbyLinks);

        ResponseMocker.mockGetResponse(client, uri, typeLinks, describedbyLinks, body);
    }

    @Test
    public void testExportVersionsOff() throws Exception {
        final URI resource1 = new URI(BASE_URI + "1");
        final URI resource1Versions = new URI(BASE_URI + "1/fcr:versions");
        final URI resource1Version1 = new URI(BASE_URI + "1/fcr:versions/version1");

        when(config.getResource()).thenReturn(resource1);
        when(config.includeVersions()).thenReturn(false);

        mockResponse(resource1, containerLinks, emptyList(), createJson(resource1));

        // Setup version responses, which should not get invoked
        final String versionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), resource1,
                resource1Version1, versionCreated, versionLabel));
        mockResponse(resource1Versions, emptyList(), emptyList(), versionsJson);
        mockResponse(resource1Version1, containerLinks, emptyList(), createJson(resource1));

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions.jsonld")));
        assertFalse(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version1.jsonld")));
    }

    @Test
    public void testExportVersionsContainers() throws Exception {
        final URI resource1 = new URI(BASE_URI + "1");
        final URI resource2 = new URI(BASE_URI + "1/2");
        final URI resource1Versions = new URI(BASE_URI + "1/fcr:versions");
        final URI resource1Version1 = new URI(BASE_URI + "1/fcr:versions/version1");
        final URI resourceVersionedChild = new URI(BASE_URI + "1/fcr:versions/version1/vChild");
        final URI resource2Versions = new URI(BASE_URI + "1/2/fcr:versions");

        when(config.getResource()).thenReturn(resource1);

        mockResponse(resource1, containerLinks, emptyList(), createJson(resource1, resource2));
        mockResponse(resource2, containerLinks, emptyList(), createJson(resource2));

        final String versionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), resource1,
                resource1Version1, versionCreated, versionLabel));
        mockResponse(resource1Versions, emptyList(), emptyList(), versionsJson);

        mockResponse(resource1Version1, containerLinks, emptyList(),
                createJson(resource1, resourceVersionedChild));
        mockResponse(resourceVersionedChild, containerLinks, emptyList(),
                createJson(resourceVersionedChild));

        ResponseMocker.mockGetResponseError(client, resource2Versions, HttpStatus.SC_NOT_FOUND);

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/2.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version1.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version1/vChild.jsonld")));
        assertFalse("Child not present in previous version should not appear in version export",
                exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version1/2.jsonld")));
    }

    @Test
    public void testExportNonversionedResource() throws Exception {
        final URI resource1 = new URI(BASE_URI + "1");
        final URI resource1Versions = new URI(BASE_URI + "1/fcr:versions");

        when(config.getResource()).thenReturn(resource1);

        mockResponse(resource1, containerLinks, emptyList(), createJson(resource1));

        ResponseMocker.mockGetResponseError(client, resource1Versions, HttpStatus.SC_NOT_FOUND);

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertFalse("Versions metadata should not be present for unversioned resource",
                exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions.jsonld")));
        assertFalse("Versions directory should not be present for unversioned resource",
                exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions")));
    }

    @Test
    public void testExportVersionedBinary() throws Exception {
        final URI binaryResc = new URI(BASE_URI + "file1");
        final URI binaryRescMetadata = new URI(BASE_URI + "file1/fcr:metadata");
        final URI binaryRescVersions = new URI(BASE_URI + "file1/fcr:versions");
        final URI binaryRescVersion = new URI(BASE_URI + "file1/fcr:versions/version1");
        final URI binaryRescMetadataVersion = new URI(BASE_URI + "file1/fcr:versions/version1/fcr:metadata");

        mockResponse(binaryResc, binaryLinks, Arrays.asList(binaryRescMetadata), "binary");
        mockResponse(binaryRescVersion, binaryLinks, Arrays.asList(binaryRescMetadataVersion), "old binary");

        final List<URI> descriptionLinks = Arrays.asList(URI.create(RDF_SOURCE.getURI()));
        mockResponse(binaryRescMetadata, descriptionLinks, Collections.emptyList(),
                createJson(binaryRescMetadata));
        mockResponse(binaryRescMetadataVersion, descriptionLinks, Collections.emptyList(),
                createJson(binaryRescMetadataVersion));

        final String versionsJson = joinJsonArray(addVersionJson(new ArrayList<>(), binaryResc,
                binaryRescVersion, versionCreated, versionLabel));
        mockResponse(binaryRescVersions, emptyList(), emptyList(), versionsJson);

        when(config.getResource()).thenReturn(binaryResc);

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1.binary")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Ametadata.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Aversions/version1.binary")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/file1/fcr%3Aversions/version1/fcr%3Ametadata.jsonld")));
    }

    @Test
    public void testExportMultipleVersionsContainers() throws Exception {
        final String version2Label = "version2";
        final String version3Label = "version_original";
        final String version2Created = "2017-05-10T00:00:00.600Z";
        final String version3Created = "2016-12-24T01:00:00.000Z";

        final URI resource = new URI(BASE_URI + "1");
        final URI resourceVersions = new URI(BASE_URI + "1/fcr:versions");
        final URI resourceVersion1 = new URI(BASE_URI + "1/fcr:versions/version1");
        final URI resourceVersion2 = new URI(BASE_URI + "1/fcr:versions/version2");
        final URI resourceVersion3 = new URI(BASE_URI + "1/fcr:versions/version_original");
        final URI resourceVersion2Child = new URI(BASE_URI + "1/fcr:versions/version2/vChild");

        when(config.getResource()).thenReturn(resource);

        mockResponse(resource, containerLinks, emptyList(), createJson(resource));

        // Add all versions to fcr:versions response
        final List<String> versionList = new ArrayList<>();
        addVersionJson(versionList, resource, resourceVersion1, versionCreated, versionLabel);
        addVersionJson(versionList, resource, resourceVersion2, version2Created, version2Label);
        addVersionJson(versionList, resource, resourceVersion3, version3Created, version3Label);
        final String versionsJson = joinJsonArray(versionList);
        mockResponse(resourceVersions, emptyList(), emptyList(), versionsJson);

        // Mock responses for version resources
        mockResponse(resourceVersion1, containerLinks, emptyList(), createJson(resourceVersion1));
        mockResponse(resourceVersion2, containerLinks, emptyList(),
                createJson(resourceVersion2, resourceVersion2Child));
        mockResponse(resourceVersion3, containerLinks, emptyList(), createJson(resourceVersion3));
        mockResponse(resourceVersion2Child, containerLinks, emptyList(), createJson(resourceVersion2Child));

        exporter.run();

        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version1.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version2.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version2/vChild.jsonld")));
        assertTrue(exporter.wroteFile(new File(basedir + "/rest/1/fcr%3Aversions/version_original.jsonld")));
    }

    private String createJson(final URI resource, final URI... children) {
        final StringBuilder json = new StringBuilder("{\"@id\":\"" + resource.toString() + "\"");
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
                "\"" + HAS_VERSION.getURI() + "\":[{\"@id\":\"" + versionUri.toString() + "\"}]}," +
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
