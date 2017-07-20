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

import static org.fcrepo.importexport.common.URITranslationUtil.remapResourceUri;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;
import java.net.URI;

import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.importer.VersionImporter.ImportEvent;
import org.fcrepo.importexport.importer.VersionImporter.ImportResource;
import org.fcrepo.importexport.importer.VersionImporter.ImportVersion;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author bbpennel
 */
public class ChronologicalImportEventIteratorTest {

    private final static URI restUri = URI.create("http://localhost:8080/rest");

    private ChronologicalImportEventIterator rescIt;

    @Mock
    private Config config;

    @Mock
    private ImportResourceFactory rescFactory;

    @Mock
    private ImportResource impResource;

    @Mock
    private ImportResource restResource;

    @Before
    public void setup() throws Exception {
        initMocks(this);

        when(config.getRdfExtension()).thenReturn(".jsonld");
        when(config.getRdfLanguage()).thenReturn("application/ld+json");
        when(config.getResource()).thenReturn(restUri);

        when(restResource.getUri()).thenReturn(restUri);
        when(rescFactory.createFromUri(eq(restUri), any(File.class), anyLong())).thenReturn(restResource);

        when(rescFactory.createFromUri(any(URI.class), any(File.class), anyLong()))
            .thenAnswer(new Answer<ImportResource>() {
                @Override
                public ImportResource answer(InvocationOnMock invocation) throws Throwable {
                    final ImportResource resc = mock(ImportResource.class);
                    final URI uri = invocation.getArgumentAt(0, URI.class);
                    final URI mappedUri = remapResourceUri(uri, null, null);
                    when(resc.getUri()).thenReturn(uri);
                    when(resc.getMappedUri()).thenReturn(mappedUri);
                    when(resc.isVersion()).thenReturn(true);
                    when(resc.getTimestamp()).thenReturn(invocation.getArgumentAt(2, Long.class));
                    return resc;
                }
            });

        when(rescFactory.createImportVersion(any(URI.class), anyLong()))
            .thenAnswer(new Answer<ImportVersion>() {
                @Override
                public ImportVersion answer(InvocationOnMock invocation) throws Throwable {
                    final ImportVersion resc = mock(ImportVersion.class);
                    when(resc.getUri()).thenReturn(invocation.getArgumentAt(0, URI.class));
                    when(resc.getMappedUri()).thenReturn(invocation.getArgumentAt(0, URI.class));
                    when(resc.getTimestamp()).thenReturn(invocation.getArgumentAt(1, Long.class));
                    return resc;
                }
            });
    }

    @Test
    public void testSingleContainer() throws Exception {
        final URI resourceUri = new URI("http://localhost:8080/rest/con1");

        final File directory = new File("src/test/resources/sample/container");
        when(config.getBaseDirectory()).thenReturn(directory);

        rescIt = new ChronologicalImportEventIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());
        final ImportEvent resc = rescIt.next();
        assertEquals(resourceUri, resc.getUri());
        assertFalse(rescIt.hasNext());
    }

    @Test
    public void testBinary() throws Exception {
        final URI resourceUri = new URI("http://localhost:8080/rest/bin1");

        final File directory = new File("src/test/resources/sample/binary");
        when(config.getBaseDirectory()).thenReturn(directory);

        rescIt = new ChronologicalImportEventIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());

        final ImportEvent resc1 = rescIt.next();
        assertEquals("http://localhost:8080/rest", resc1.getUri().toString());
        final ImportEvent resc2 = rescIt.next();
        assertEquals(resourceUri, resc2.getUri());

        assertFalse(rescIt.hasNext());
    }

    @Test
    public void testVersions() throws Exception {
        when(config.includeVersions()).thenReturn(true);

        final URI con1Uri = new URI("http://localhost:8080/fcrepo/rest/con1");
        final URI con1VOriginalUri = new URI(
                "http://localhost:8080/fcrepo/rest/con1/fcr:versions/version_original");
        final URI con1V1Uri = new URI(
                "http://localhost:8080/fcrepo/rest/con1/fcr:versions/version_1");
        final URI con2VOriginalUri = new URI(
                "http://localhost:8080/fcrepo/rest/con1/fcr:versions/version_original/con2");
        final URI bin1Uri = new URI("http://localhost:8080/fcrepo/rest/con1/bin1");
        final URI bin1V1Uri = new URI(
                "http://localhost:8080/fcrepo/rest/con1/fcr:versions/version_1/bin1");

        final File directory = new File("src/test/resources/sample/versioned");
        when(config.getBaseDirectory()).thenReturn(directory);

        rescIt = new ChronologicalImportEventIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());

        assertEquals(con1VOriginalUri, rescIt.next().getUri());
        assertEquals(con2VOriginalUri, rescIt.next().getUri());
        ImportEvent version1 = rescIt.next();
        assertTrue("Expected version creation event", version1 instanceof ImportVersion);
        assertEquals(con1VOriginalUri, version1.getUri());

        assertEquals(con1V1Uri, rescIt.next().getUri());
        assertEquals(bin1V1Uri, rescIt.next().getUri());
        ImportEvent version2 = rescIt.next();
        assertTrue("Expected version creation event", version2 instanceof ImportVersion);
        assertEquals(con1V1Uri, version2.getUri());

        assertEquals(con1Uri, rescIt.next().getUri());
        assertEquals(bin1Uri, rescIt.next().getUri());

        assertFalse(rescIt.hasNext());
    }

    @Test
    public void testExcludeVersions() throws Exception {
        when(config.includeVersions()).thenReturn(false);

        final URI con1Uri = new URI("http://localhost:8080/fcrepo/rest/con1");
        final URI bin1Uri = new URI("http://localhost:8080/fcrepo/rest/con1/bin1");

        final File directory = new File("src/test/resources/sample/versioned");
        when(config.getBaseDirectory()).thenReturn(directory);

        rescIt = new ChronologicalImportEventIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());

        assertEquals(con1Uri, rescIt.next().getUri());
        assertEquals(bin1Uri, rescIt.next().getUri());

        assertFalse(rescIt.hasNext());
    }

    @Test
    public void testUnmodifiedVersions() throws Exception {
        when(config.includeVersions()).thenReturn(true);

        final File directory = new File("src/test/resources/sample/unmodified_version");
        when(config.getBaseDirectory()).thenReturn(directory);

        final URI con1Uri = new URI("http://localhost:8080/rest/v_con1");
        final URI con1VOriginalUri = new URI(
                "http://localhost:8080/rest/v_con1/fcr:versions/version_original");
        final URI con1VUnmodUri = new URI(
                "http://localhost:8080/rest/v_con1/fcr:versions/version_unchanged");
        final URI child1Uri = new URI("http://localhost:8080/rest/v_con1/child1");
        final URI child1VOriginalUri = new URI(
                "http://localhost:8080/rest/v_con1/fcr:versions/version_original/child1");

        rescIt = new ChronologicalImportEventIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());

        assertEquals(con1Uri, rescIt.next().getUri());
        assertEquals(child1VOriginalUri, rescIt.next().getUri());

        ImportEvent versionOriginal = rescIt.next();
        assertTrue("Expected version creation event", versionOriginal instanceof ImportVersion);
        assertEquals(con1VOriginalUri, versionOriginal.getUri());

        ImportEvent versionUnmod = rescIt.next();
        assertTrue("Expected version creation event", versionUnmod instanceof ImportVersion);
        assertEquals(con1VUnmodUri, versionUnmod.getUri());

        assertEquals(child1Uri, rescIt.next().getUri());

        assertFalse(rescIt.hasNext());
    }
}
