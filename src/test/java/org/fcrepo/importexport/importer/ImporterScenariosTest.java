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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fcrepo.importexport.common.FcrepoConstants.CONTAINS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

import org.fcrepo.client.DeleteBuilder;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.client.GetBuilder;
import org.fcrepo.client.HeadBuilder;
import org.fcrepo.client.PostBuilder;
import org.fcrepo.client.PutBuilder;
import org.fcrepo.importexport.common.AuthenticationRequiredRuntimeException;
import org.fcrepo.importexport.common.Config;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Importer tests that build the export directory on the fly to exercise skip and error handling paths.
 *
 * @author dfield
 */
public class ImporterScenariosTest {

    private static final String BASE = "http://localhost:8080/rest";
    private static final URI ROOT = URI.create(BASE);
    private static final URI DESCRIPTION = URI.create(BASE + "/bin/fcr:metadata");
    private static final String TITLE = "<http://purl.org/dc/terms/title>";
    private static final String ROOT_TTL = "<" + BASE + "> a "
            + "<http://fedora.info/definitions/v4/repository#RepositoryRoot> .";

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private Config config;
    private FcrepoClient client;
    private FcrepoClient.FcrepoClientBuilder clientBuilder;
    private HeadBuilder headBuilder;
    private FcrepoResponse headResponse;
    private GetBuilder getBuilder;
    private PutBuilder putBuilder;
    private PostBuilder postBuilder;
    private DeleteBuilder deleteBuilder;

    @Before
    public void setUp() throws Exception {
        config = new Config();
        config.setMode("import");
        config.setBaseDirectory(tmp.getRoot().getAbsolutePath());
        config.setResource(ROOT);
        config.setIncludeBinaries(true);

        client = mock(FcrepoClient.class);
        clientBuilder = mock(FcrepoClient.FcrepoClientBuilder.class);
        when(clientBuilder.build()).thenReturn(client);

        headBuilder = mock(HeadBuilder.class, RETURNS_SELF);
        headResponse = response(200, "");
        when(client.head(any())).thenReturn(headBuilder);
        when(headBuilder.perform()).thenReturn(headResponse);

        getBuilder = mock(GetBuilder.class, RETURNS_SELF);
        when(client.get(any())).thenReturn(getBuilder);
        when(getBuilder.perform()).thenAnswer(invocation -> response(200, ROOT_TTL));

        putBuilder = putBuilder(201);
        when(client.put(any())).thenReturn(putBuilder);

        postBuilder = mock(PostBuilder.class, RETURNS_SELF);
        when(client.post(any())).thenReturn(postBuilder);
        when(postBuilder.perform()).thenAnswer(invocation -> response(201, ""));

        deleteBuilder = mock(DeleteBuilder.class, RETURNS_SELF);
        when(client.delete(any())).thenReturn(deleteBuilder);
        when(deleteBuilder.perform()).thenAnswer(invocation -> response(204, ""));

        write("rest.ttl", "<" + BASE + "> " + TITLE + " \"root\" .");
    }

    private static FcrepoResponse response(final int status, final String body) {
        final FcrepoResponse response = mock(FcrepoResponse.class);
        when(response.getStatusCode()).thenReturn(status);
        when(response.getBody()).thenReturn(new ByteArrayInputStream(body.getBytes(UTF_8)));
        when(response.getLinkHeaders("describedby")).thenReturn(Collections.singletonList(DESCRIPTION));
        return response;
    }

    private static PutBuilder putBuilder(final int status) throws FcrepoOperationFailedException {
        final PutBuilder builder = mock(PutBuilder.class, RETURNS_SELF);
        when(builder.perform()).thenAnswer(invocation -> response(status, "response body"));
        return builder;
    }

    private File write(final String path, final String content) throws IOException {
        final File file = new File(tmp.getRoot(), path);
        file.getParentFile().mkdirs();
        Files.write(file.toPath(), content.getBytes(UTF_8));
        return file;
    }

    private void writeBinary(final String name) throws IOException {
        write("rest/" + name + ".binary", "binary content");
        write("rest/" + name + "/fcr%3Ametadata.ttl", "<" + BASE + "/" + name + "> a "
                + "<http://www.w3.org/ns/ldp#NonRDFSource> ;\n"
                + "  <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType> \"text/plain\" ;\n"
                + "  <http://www.loc.gov/premis/rdf/v1#hasMessageDigest> <urn:sha1:abc123> .");
    }

    private Importer importer() {
        return new Importer(config, clientBuilder);
    }

    @Test
    public void testSkipsUnexpectedFilesMementosAndAcls() throws Exception {
        write("rest/notes.txt", "not rdf");
        write("rest/memento.ttl", "<" + BASE + "/memento> " + TITLE + " \"memento\" .");
        write("rest/memento.ttl.headers",
                "{\"Link\":[\"<http://mementoweb.org/ns#Memento>; rel=\\\"type\\\"\"]}");
        write("rest/foo/fcr%3Aacl.ttl",
                "<" + BASE + "/foo/fcr:acl> a <http://fedora.info/definitions/v4/webac#Acl> .");

        importer().run();

        verify(client).put(ROOT);
        verify(client, never()).put(URI.create(BASE + "/memento"));
        verify(client, never()).put(URI.create(BASE + "/foo/fcr:acl"));
        verify(client, never()).post(any());
    }

    @Test
    public void testImportsMementoWithoutTimemapLink() throws Exception {
        config.setIncludeVersions(true);
        write("rest/memento.ttl", "<" + BASE + "/memento> " + TITLE + " \"memento\" .");
        write("rest/memento.ttl.headers",
                "{\"Link\":[\"<http://mementoweb.org/ns#Memento>; rel=\\\"type\\\"\"]}");

        importer().run();

        verify(client).post(null);
        verify(postBuilder).addHeader("Memento-Datetime", null);
    }

    @Test
    public void testErrorResponseIsLogged() throws Exception {
        write("rest/child.ttl", "<" + BASE + "/child> " + TITLE + " \"child\" .");
        final PutBuilder failing = putBuilder(500);
        when(client.put(eq(URI.create(BASE + "/child")))).thenReturn(failing);

        importer().run();

        verify(failing).perform();
        verify(client).put(ROOT);
    }

    @Test
    public void testClientExceptionOnImport() throws Exception {
        write("rest/child.ttl", "<" + BASE + "/child> " + TITLE + " \"child\" .");
        when(putBuilder.perform()).thenThrow(new FcrepoOperationFailedException(ROOT, 500, "failed"));

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().run());
        assertTrue(e.getMessage().startsWith("Error importing"));
    }

    @Test
    public void testMalformedRdf() throws Exception {
        write("rest/bad.ttl", "this is not turtle");

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().run());
        assertTrue(e.getMessage().startsWith("Error parsing RDF"));
    }

    @Test
    public void testMalformedHeaders() throws Exception {
        write("rest/child.ttl", "<" + BASE + "/child> " + TITLE + " \"child\" .");
        write("rest/child.ttl.headers", "this is not json");

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().run());
        assertTrue(e.getMessage().startsWith("Error reading or parsing headers file"));
    }

    @Test
    public void testPlaceholderCreationFailure() throws Exception {
        final HeadBuilder failing = mock(HeadBuilder.class, RETURNS_SELF);
        when(failing.perform()).thenThrow(new FcrepoOperationFailedException(ROOT, 500, "failed"));
        when(client.head(any())).thenReturn(failing);
        when(failing.disableRedirects()).thenReturn(headBuilder);

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().run());
        assertEquals("Unable to create placeholder " + ROOT, e.getMessage());
    }

    @Test
    public void testFindRepositoryRootSkipsMissingResources() throws Exception {
        final URI missing = URI.create(BASE + "/missing");
        final HeadBuilder notFound = mock(HeadBuilder.class, RETURNS_SELF);
        when(notFound.perform()).thenAnswer(invocation -> response(404, ""));
        when(client.head(eq(missing))).thenReturn(notFound);

        assertEquals(ROOT, importer().findRepositoryRoot(missing));
    }

    @Test
    public void testFindRepositoryRootClientFailure() throws Exception {
        when(headBuilder.perform()).thenThrow(new FcrepoOperationFailedException(ROOT, 500, "failed"));

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().findRepositoryRoot(ROOT));
        assertEquals("Error finding repository root " + ROOT, e.getMessage());
    }

    @Test
    public void testFindRepositoryRootIOFailure() throws Exception {
        doThrow(new IOException("closed")).when(headResponse).close();

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().findRepositoryRoot(ROOT));
        assertEquals("Error finding repository root " + ROOT, e.getMessage());
    }

    @Test
    public void testOverwriteContainerTombstone() throws Exception {
        config.setOverwriteTombstones(true);
        final URI child = URI.create(BASE + "/child");
        final URI tombstone = URI.create(BASE + "/child/fcr:tombstone");
        write("rest/child.ttl", "<" + child + "> " + TITLE + " \"child\" .");

        final FcrepoResponse gone = response(410, "");
        when(gone.getLinkHeaders("hasTombstone")).thenReturn(Collections.singletonList(tombstone));
        final PutBuilder childBuilder = mock(PutBuilder.class, RETURNS_SELF);
        when(childBuilder.perform()).thenReturn(gone).thenAnswer(invocation -> response(201, ""));
        when(client.put(eq(child))).thenReturn(childBuilder);

        importer().run();

        verify(client).delete(tombstone);
    }

    @Test
    public void testOverwriteTombstoneFromParent() throws Exception {
        config.setOverwriteTombstones(true);
        final URI child = URI.create(BASE + "/child");
        final URI tombstone = URI.create(BASE + "/fcr:tombstone");
        write("rest/child.ttl", "<" + child + "> " + TITLE + " \"child\" .");

        final FcrepoResponse gone = response(410, "");
        when(gone.getLinkHeaders("hasTombstone")).thenReturn(Arrays.asList((URI) null));
        when(gone.getUrl()).thenReturn(URI.create(child + "/"));
        when(headResponse.getLinkHeaders("hasTombstone")).thenReturn(Collections.singletonList(tombstone));
        final PutBuilder childBuilder = mock(PutBuilder.class, RETURNS_SELF);
        when(childBuilder.perform()).thenReturn(gone).thenAnswer(invocation -> response(201, ""));
        when(client.put(eq(child))).thenReturn(childBuilder);

        importer().run();

        verify(gone).getUrl();
        verify(client).delete(tombstone);
    }

    @Test
    public void testOverwriteBinaryTombstone() throws Exception {
        config.setOverwriteTombstones(true);
        final URI binary = URI.create(BASE + "/bin");
        final URI tombstone = URI.create(BASE + "/bin/fcr:tombstone");
        writeBinary("bin");

        final FcrepoResponse gone = response(410, "");
        when(gone.getLinkHeaders("hasTombstone")).thenReturn(Collections.singletonList(tombstone));
        final PutBuilder binaryBuilder = mock(PutBuilder.class, RETURNS_SELF);
        when(binaryBuilder.perform()).thenReturn(gone).thenAnswer(invocation -> response(201, ""));
        when(client.put(eq(binary))).thenReturn(binaryBuilder);

        importer().run();

        verify(client).delete(tombstone);
        verify(binaryBuilder, times(2)).digestSha1("abc123");
    }

    @Test
    public void testBinaryImportError() throws Exception {
        final URI binary = URI.create(BASE + "/bin");
        writeBinary("bin");
        final PutBuilder binaryBuilder = putBuilder(500);
        when(client.put(eq(binary))).thenReturn(binaryBuilder);

        importer().run();

        verify(binaryBuilder).perform();
        verify(client, never()).put(URI.create(BASE + "/bin/fcr:metadata"));
    }

    private void writeMembershipResources() throws IOException {
        write("rest/indirect.ttl", "<" + BASE + "/indirect> "
                + "<http://www.w3.org/ns/ldp#membershipResource> <" + BASE + "/members> .");
        write("rest/members.ttl", "<" + BASE + "/members> " + TITLE + " \"members\" .");
    }

    @Test
    public void testMembershipResourceUnauthorized() throws Exception {
        writeMembershipResources();
        final PutBuilder membersBuilder = putBuilder(401);
        when(client.put(eq(URI.create(BASE + "/members")))).thenReturn(membersBuilder);

        assertThrows(AuthenticationRequiredRuntimeException.class, () -> importer().run());
    }

    @Test
    public void testMembershipResourceError() throws Exception {
        writeMembershipResources();
        final PutBuilder membersBuilder = putBuilder(500);
        when(client.put(eq(URI.create(BASE + "/members")))).thenReturn(membersBuilder);

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().run());
        assertTrue(e.getMessage().startsWith("Error while importing membership resource"));
    }

    @Test
    public void testMembershipResourceClientFailure() throws Exception {
        writeMembershipResources();
        final GetBuilder failing = mock(GetBuilder.class, RETURNS_SELF);
        when(failing.perform()).thenThrow(new FcrepoOperationFailedException(ROOT, 500, "failed"));
        when(client.get(eq(URI.create(BASE + "/members")))).thenReturn(failing);

        final RuntimeException e = assertThrows(RuntimeException.class, () -> importer().run());
        assertTrue(e.getMessage().startsWith("Error importing"));
    }

    @Test
    public void testImportsRelatedResources() throws Exception {
        final String link = "<http://example.org/link>";
        config.setResource(BASE + "/col");
        config.setPredicates(new String[]{CONTAINS.getURI(), "http://example.org/link"});
        write("rest/col.ttl", "<" + BASE + "/col> " + link + " <" + BASE + "/other> , <" + BASE + "/bin> .");
        write("rest/col/a.ttl", "<" + BASE + "/col/a> " + link + " <" + BASE + "/col/b> .");
        write("rest/col/b.ttl", "<" + BASE + "/col/b> " + TITLE + " \"b\" .");
        write("rest/other.ttl", "<" + BASE + "/other> " + TITLE + " \"other\" .");
        writeBinary("bin");

        importer().run();

        verify(client).put(URI.create(BASE + "/col/b"));
        verify(client).put(URI.create(BASE + "/other"));
        verify(client).put(URI.create(BASE + "/bin"));
    }

    @Test
    public void testPlaceholdersForReferencedResources() throws Exception {
        final URI referenced = URI.create(BASE + "/b");
        write("rest.ttl", "<" + BASE + "> <http://purl.org/dc/terms/relation> <" + referenced + "> , <"
                + BASE + "/c/> .");
        write("rest/b.ttl", "<" + referenced + "> " + TITLE + " \"b\" .");

        final HeadBuilder notFound = mock(HeadBuilder.class, RETURNS_SELF);
        when(notFound.perform()).thenAnswer(invocation -> response(404, ""));
        when(client.head(eq(referenced))).thenReturn(notFound);
        when(client.head(eq(URI.create(BASE + "/c/")))).thenReturn(notFound);
        final PutBuilder referencedBuilder = putBuilder(500);
        when(client.put(eq(referenced))).thenReturn(referencedBuilder);

        importer().run();

        // once for the placeholder and once when importing b.ttl
        verify(referencedBuilder, times(2)).body(isA(InputStream.class), eq("text/turtle"));
        verify(client, never()).put(URI.create(BASE + "/c/"));
    }

    @Test
    public void testSerializedBag() throws Exception {
        final File serialized = new File(tmp.getRoot(), "bag-tar.tar");
        Files.copy(new File("src/test/resources/sample/compress/bag-tar.tar").toPath(), serialized.toPath());
        config.setBaseDirectory(serialized.getAbsolutePath());
        config.setBagProfile("default");

        importer();

        assertEquals(new File(tmp.getRoot(), "bag-tar/data").getAbsoluteFile(),
                config.getBaseDirectory().getAbsoluteFile());
    }
}
