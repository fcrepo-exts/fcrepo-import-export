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


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test that zip, tar, and tar+gz extraction works as expected
 *
 * @author mikejritter
 * @since 2020-02-13
 */
@RunWith(Parameterized.class)
public class BagDeserializerTest {

    @Parameters(name = "extract {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {"bag-tar.tar", "bag-tar"}, {"bag-zip.zip", "bag-zip"}, {"bag-gzip.tar.gz", "bag-gzip"},
            {"bag-tar-mismatch.tar", "bag-tar"}
        });
    }

    public static final String BEYONDTHEREPOSITORY_JSON = "src/main/resources/profiles/beyondtherepository.json";
    public static final String BAG_INFO_TXT = "bag-info.txt";
    public static final String DATA_DIR = "data";
    private final String group = "compress";
    private Path target;

    private final String archive;
    private final String expectedDir;

    public BagDeserializerTest(final String archive, final String expectedDir) {
        this.archive = archive;
        this.expectedDir = expectedDir;
    }

    @Before
    public void setup() throws URISyntaxException {
        final URL sample = this.getClass().getClassLoader().getResource("sample");
        target = Paths.get(Objects.requireNonNull(sample).toURI());
        assertNotNull(target);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(target.resolve(group).resolve(expectedDir).toFile());
    }

    @Test
    public void testExtract() {
        final Path path = target.resolve(group).resolve(archive);
        try {
            final BagProfile profile = new BagProfile(Files.newInputStream(Paths.get(BEYONDTHEREPOSITORY_JSON)));
            final BagDeserializer deserializer = SerializationSupport.deserializerFor(path, profile);
            deserializer.deserialize(path);
        } catch (IOException e) {
            fail("Unexpected exception:\n" + e.getMessage());
        }

        final Path bag = target.resolve(group).resolve(expectedDir);
        assertTrue(Files.exists(bag));
        assertTrue(Files.exists(bag.resolve(BAG_INFO_TXT)));
        assertTrue(Files.exists(bag.resolve(DATA_DIR)));
        assertTrue(Files.isDirectory(bag.resolve(DATA_DIR)));
    }

}
