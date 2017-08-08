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

import static org.fcrepo.importexport.common.ModelUtils.mapRdfStream;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.fcrepo.importexport.common.Config;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author bbpennel
 *
 */
public class VersionDiffDeletionGeneratorTest {

    private VersionDiffDeletionGenerator generator;

    private Config config;

    @Before
    public void init() {
        config = new Config();
        config.setRdfLanguage("application/ld+json");

        generator = new VersionDiffDeletionGenerator(config);
    }

    @Test
    public void testUnchangedVersion() throws Exception {
        final String baseDir = "src/test/resources/sample/versioning/unmodified";
        config.setBaseDirectory(baseDir);

        final Model model = getModel(baseDir + "/rest/v_con1/fcr%3Aversions.jsonld");

        final List<ImportDeletion> deletions = generator.generateImportDeletions(model);
        assertEquals(0, deletions.size());
    }

    @Test
    public void testContainerAdded() throws Exception {
        // Ensure that containers being added in later versions don't trigger any deletion
        final String baseDir = "src/test/resources/sample/versioning/container_added";
        config.setBaseDirectory(baseDir);

        final Model model = getModel(baseDir + "/rest/con1/fcr%3Aversions.jsonld");

        final List<ImportDeletion> deletions = generator.generateImportDeletions(model);
        assertEquals(0, deletions.size());
    }

    @Test
    public void testContainerRestoredAdded() throws Exception {
        // A container that is removed in a version then added back in should get deleted
        final URI child1Uri = URI.create("http://localhost:8080/rest/con1/child1");

        final String baseDir = "src/test/resources/sample/versioning/container_restored";
        config.setBaseDirectory(baseDir);

        final Model model = getModel(baseDir + "/rest/con1/fcr%3Aversions.jsonld");

        final List<ImportDeletion> deletions = generator.generateImportDeletions(model);
        assertEquals(1, deletions.size());
        assertEquals(child1Uri, deletions.get(0).getUri());
    }

    @Test
    public void testContainerRemovedInHeadVersion() throws Exception {
        final URI con2Uri = URI.create("http://localhost:8080/fcrepo/rest/con1/con2");

        final String baseDir = "src/test/resources/sample/versioning/removed_in_head";
        config.setBaseDirectory(baseDir);

        final Model model = getModel(baseDir + "/fcrepo/rest/con1/fcr%3Aversions.jsonld");

        final List<ImportDeletion> deletions = generator.generateImportDeletions(model);
        assertEquals(1, deletions.size());
        assertEquals(con2Uri, deletions.get(0).getUri());
    }

    @Test
    public void testBinaryRemoved() throws Exception {
        final URI bin1Uri = URI.create("http://localhost:8080/fcrepo/rest/con1/bin1");

        final String baseDir = "src/test/resources/sample/versioning/binary_removed";
        config.setBaseDirectory(baseDir);

        final Model model = getModel(baseDir + "/fcrepo/rest/con1/fcr%3Aversions.jsonld");

        final List<ImportDeletion> deletions = generator.generateImportDeletions(model);
        assertEquals(1, deletions.size());
        assertEquals(bin1Uri, deletions.get(0).getUri());
    }

    private Model getModel(final String fileUri) throws FileNotFoundException, IOException {
        return mapRdfStream(new FileInputStream(new File(fileUri)), config);
    }
}
