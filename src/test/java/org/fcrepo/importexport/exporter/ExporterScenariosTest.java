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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collections;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.importexport.common.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exporter tests for configuration errors and failures while exporting.
 *
 * @author dfield
 */
public class ExporterScenariosTest {

    private static final String BASE = "http://localhost:8080/rest";
    private static final URI ROOT = URI.create(BASE);
    private static final URI RESOURCE = URI.create(BASE + "/1");

    @TempDir
    public File tmp;

    private Config config;
    private FcrepoClient client;
    private FcrepoClient.FcrepoClientBuilder clientBuilder;
    private HeadBuilder headBuilder;
    private FcrepoResponse headResponse;
    private GetBuilder getBuilder;

    @BeforeEach
    public void setUp() throws Exception {
        config = new Config();
        config.setMode("export");
        config.setBaseDirectory(new File(tmp, "export").getAbsolutePath());
        config.setResource(RESOURCE);
        config.setRepositoryRoot(ROOT);
        config.setThreadCount(1);

        client = mock(FcrepoClient.class);
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        when(clientBuilder.build()).thenReturn(client);

        headBuilder = mock(HeadBuilder.class, RETURNS_SELF);
        headResponse = response(200, "");
        when(client.head(any())).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);

        getBuilder = mock(GetBuilder.class, RETURNS_SELF);
        when(client.get(any())).thenReturn(getBuilder);
        when(getBuilder.perform()).thenAnswer(invocation -> response(200, ""));
    }

    private static FcrepoResponse response(final int status, final String body) {
        final FcrepoResponse response = mock(FcrepoResponse.class);
        when(response.getStatusCode()).thenReturn(status);
        when(response.getBody()).thenReturn(new ByteArrayInputStream(body.getBytes(UTF_8)));
        return response;
    }

    private void useBagProfile(final String profile, final String configPath) {
        config.setBagProfile(profile);
        config.setBagConfigPath(configPath);
    }

    @Test
    public void testMissingBagConfigFile() {
        useBagProfile("default", new File(tmp, "missing.yml").getPath());
        final RuntimeException e = assertThrows(RuntimeException.class, () -> new Exporter(config, clientBuilder));
        assertTrue(e.getMessage().startsWith("Error reading bag profile"));
    }

    @Test
    public void testNullBagConfigPath() {
        useBagProfile("default", null);
        final RuntimeException e = assertThrows(RuntimeException.class, () -> new Exporter(config, clientBuilder));
        assertEquals("The bag config path must not be null.", e.getMessage());
    }

    @Test
    public void testGenerateChecksumsForAllAlgorithms() throws Exception {
        useBagProfile("beyondtherepository", "src/test/resources/configs/bagit-config-no-aptrust.yml");
        config.setBagAlgorithms(new String[]{"md5", "sha1", "sha256", "sha512"});
        final Exporter exporter = new Exporter(config, clientBuilder);

        final File file = newFile(tmp, "content.txt");
        Files.write(file.toPath(), "content".getBytes(UTF_8));
        exporter.generateChecksums(file);
        // a missing file is logged rather than thrown
        exporter.generateChecksums(new File(tmp, "missing.txt"));
    }

    @Test
    public void testRepositoryRootLookupFailure() throws Exception {
        config.setRepositoryRoot((URI) null);
        when(headBuilder.perform()).thenThrow(new FcrepoOperationFailedException(RESOURCE, 500, "failed"));

        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> new Exporter(config, clientBuilder).run());
        assertEquals("Failed to locate the root of the repository being exported", e.getMessage());
    }

    @Test
    public void testMissingAclIsIgnored() throws Exception {
        final URI acl = URI.create(RESOURCE + "/fcr:acl");
        config.setResource(acl);
        when(headResponse.getStatusCode()).thenReturn(404);

        new Exporter(config, clientBuilder).run();

        verify(client, never()).get(any());
    }

    @Test
    public void testRootAclIsNotExported() throws Exception {
        config.setResource(URI.create(BASE + "/fcr:acl"));

        new Exporter(config, clientBuilder).run();

        verify(client, never()).get(any());
    }

    @Test
    public void testUnknownResourceTypeIsNotExported() throws Exception {
        new Exporter(config, clientBuilder).run();

        verify(client, never()).get(any());
    }

    @Test
    public void testClientFailureIsLogged() throws Exception {
        when(headBuilder.perform()).thenThrow(new FcrepoOperationFailedException(RESOURCE, 500, "failed"));

        new Exporter(config, clientBuilder).run();

        verify(client, never()).get(any());
    }

    @Test
    public void testIOFailureIsLogged() throws Exception {
        doThrow(new IOException("closed")).when(headResponse).close();

        new Exporter(config, clientBuilder).run();

        verify(headResponse).close();
    }

    @Test
    public void testRuntimeFailureIsLogged() throws Exception {
        when(headResponse.getStatusCode()).thenReturn(500);

        new Exporter(config, clientBuilder).run();

        verify(client, never()).get(any());
    }

    @Test
    public void testSkippedTombstoneDuringRdfExport() throws Exception {
        config.setSkipTombstoneErrors(true);
        when(headResponse.getLinkHeaders("type")).thenReturn(Collections.singletonList(URI.create(CONTAINER.getURI())));
        when(getBuilder.perform()).thenAnswer(invocation -> response(410, ""));

        new Exporter(config, clientBuilder).run();

        verify(client).get(eq(RESOURCE));
        assertFalse(new File(config.getBaseDirectory(), "rest/1.ttl").exists());
    }

    @Test
    public void testExportAfterShutdownIsRejected() throws Exception {
        final Exporter exporter = new Exporter(config, clientBuilder);
        exporter.run();

        exporter.export(URI.create(BASE + "/late"));

        verify(client, never()).head(URI.create(BASE + "/late"));
    }

    private static File newFile(final File parent, final String child) throws IOException {
        final File result = new File(parent, child);
        result.createNewFile();
        return result;
    }
}
