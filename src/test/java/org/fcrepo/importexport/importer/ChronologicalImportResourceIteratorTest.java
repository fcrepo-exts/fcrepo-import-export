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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;
import java.net.URI;

import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.importer.VersionImporter.ImportResource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * 
 * @author bbpennel
 *
 */
public class ChronologicalImportResourceIteratorTest {

    private final static URI restUri = URI.create("http://localhost:8080/rest");

    private ChronologicalImportResourceIterator rescIt;

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
        when(rescFactory.createFromUri(eq(restUri))).thenReturn(restResource);
    }

    @Test
    public void testSingleContainer() throws Exception {
        final URI resourceUri = new URI("http://localhost:8080/rest/con1");

        final File directory = new File("src/test/resources/sample/container");
        when(config.getBaseDirectory()).thenReturn(directory);

        when(rescFactory.createFromUri(eq(resourceUri))).thenReturn(impResource);
        when(impResource.getUri()).thenReturn(resourceUri);

        rescIt = new ChronologicalImportResourceIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());
        final ImportResource resc = rescIt.next();
        assertEquals(resourceUri, resc.getUri());
        assertFalse(rescIt.hasNext());
    }

    @Test
    public void testBinary() throws Exception {
        final URI resourceUri = new URI("http://localhost:8080/rest/bin1");

        final File directory = new File("src/test/resources/sample/binary");
        when(config.getBaseDirectory()).thenReturn(directory);

        when(rescFactory.createFromUri(eq(resourceUri))).thenReturn(impResource);
        when(impResource.getUri()).thenReturn(resourceUri);

        rescIt = new ChronologicalImportResourceIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());

        final ImportResource resc1 = rescIt.next();
        assertEquals("http://localhost:8080/rest", resc1.getUri().toString());
        final ImportResource resc2 = rescIt.next();
        assertEquals(resourceUri, resc2.getUri());

        assertFalse(rescIt.hasNext());
    }

    @Test
    public void testVersions() throws Exception {
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

        when(rescFactory.createFromUri(any(URI.class))).thenAnswer(new Answer<ImportResource>() {
            @Override
            public ImportResource answer(InvocationOnMock invocation) throws Throwable {
                ImportResource resc = mock(ImportResource.class);
                when(resc.getUri()).thenReturn(invocation.getArgumentAt(0, URI.class));
                return resc;
            }
        });

        rescIt = new ChronologicalImportResourceIterator(config, rescFactory);

        assertTrue(rescIt.hasNext());

        assertEquals(con1VOriginalUri, rescIt.next().getUri());
        assertEquals(con2VOriginalUri, rescIt.next().getUri());
        assertEquals(con1V1Uri, rescIt.next().getUri());
        assertEquals(bin1V1Uri, rescIt.next().getUri());
        assertEquals(con1Uri, rescIt.next().getUri());
        assertEquals(bin1Uri, rescIt.next().getUri());

        assertFalse(rescIt.hasNext());
    }
}
