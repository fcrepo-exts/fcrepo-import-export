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

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;
import java.util.List;

import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.URITranslationUtil;
import org.junit.Before;
import org.mockito.Mock;

/**
 * 
 * @author bbpennel
 *
 */
public class ImportResourceFactoryTest {
    private final String URI_BASE = "http://example.com/";

    private ImportResourceFactory factory;

    @Mock
    private URITranslationUtil uriTranslator;

    @Mock
    private Config config;

    @Before
    public void setup() throws Exception {
        initMocks(this);

        when(config.getRdfExtension()).thenReturn(".jsonld");

        factory = new ImportResourceFactory(config);
    }

//    @Test
//    public void testCreateFromDirectorySingleContainer() throws Exception {
//        final File directory = new File("src/test/resources/sample/binary");
//        final URI baseUri = URI.create(URI_BASE + "rest");
//        when(uriTranslator.uriForFile(any(File.class))).thenReturn(baseUri);
//
//        final List<ImportResource> resources = factory.createFromDirectory(directory);
//
//        assertEquals("Only one resource should be present", 1, resources.size());
//        final ImportResource impResource = resources.get(0);
//        final List<File> files = impResource.getFiles();
//
//        assertEquals(2, files.size());
//        assertTrue(containsFilename(files, "rest.jsonld"));
//        assertTrue(containsFilename(files, "rest"));
//
//        assertEquals(baseUri, impResource.getUri());
//    }
//
//    @Test
//    public void testCreateFromDirectoryBinary() throws Exception {
//        final File directory = new File("src/test/resources/sample/binary/rest");
//
//        final List<ImportResource> resources = factory.createFromDirectory(directory);
//
//        assertEquals("Only one resource should be present", 1, resources.size());
//        final List<File> files = resources.get(0).getFiles();
//        assertEquals(2, files.size());
//        assertTrue(containsFilename(files, "bin1"));
//        assertTrue(containsFilename(files, "bin1.binary"));
//    }
//
//    @Test
//    public void testCreateFromDirectoryContainerSolo() throws Exception {
//        final File directory = new File("src/test/resources/sample/container/rest");
//
//        final List<ImportResource> resources = factory.createFromDirectory(directory);
//
//        assertEquals("Only one resource should be present", 1, resources.size());
//        final List<File> files = resources.get(0).getFiles();
//        assertEquals(1, files.size());
//        assertTrue(containsFilename(files, "con1.jsonld"));
//    }
//
//    @Test
//    public void testCreateFromDirectoryMultipleResources() throws Exception {
//        when(config.getRdfExtension()).thenReturn(".ttl");
//
//        final File directory = new File("src/test/resources/sample/bag/data/fcrepo/rest");
//
//        final List<ImportResource> resources = factory.createFromDirectory(directory);
//
//        assertEquals("Three resources expected", 3, resources.size());
//
//        final ImportResource binary1 = getResourceById(resources, "image0");
//        assertNotNull(binary1);
//        final List<File> bin1Files = binary1.getFiles();
//        assertEquals(2, bin1Files.size());
//        assertTrue(containsFilename(bin1Files, "image0.binary"));
//        assertTrue(containsFilename(bin1Files, "image0"));
//
//        final ImportResource container = getResourceById(resources, "testBagImport");
//        assertNotNull(container);
//        final List<File> containerFiles = container.getFiles();
//        assertEquals(1, containerFiles.size());
//        assertTrue(containsFilename(containerFiles, "testBagImport.ttl"));
//    }

    private ImportResource getResourceById(final List<ImportResource> resources, final String id) {
        return resources.stream().filter(r -> r.getId().equals(id)).findFirst().orElse(null);
    }

    private boolean containsFilename(final List<File> files, final String filename) {
        return files.stream().filter(f -> f.getName().equals(filename)).findFirst().isPresent();
    }
}
