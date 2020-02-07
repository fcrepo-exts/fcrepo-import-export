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


import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class BagDeserializerTest {

    public static final String BEYONDTHEREPOSITORY_JSON = "src/main/resources/profiles/beyondtherepository.json";
    public static final String BAG_INFO_TXT = "bag-info.txt";
    public static final String DATA_DIR = "data";
    private final String expectedDir = "example";
    private final String group = "compress";
    private Path target;

    @Before
    public void setup() throws URISyntaxException {
        final URL sample = this.getClass().getClassLoader().getResource("sample");
        target = Paths.get(Objects.requireNonNull(sample).toURI());
        Assert.assertNotNull(target);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(target.resolve(group).resolve(expectedDir).toFile());
    }

    @Test
    public void testExtractZip() {
        final String serializedBag = expectedDir + ".zip";
        final Path path = target.resolve(group).resolve(serializedBag);
        try {
            final String contentType = Files.probeContentType(path);
            final BagProfile profile = new BagProfile(Files.newInputStream(
                    Paths.get(BEYONDTHEREPOSITORY_JSON)));
            final BagDeserializer deserializer = SerializationSupport.deserializerFor(contentType, profile);
            deserializer.deserialize(path);
        } catch (IOException e) {
            Assert.fail("Unexpected exception:\n" + e.getMessage());
        }

        final Path bag = target.resolve(group).resolve(expectedDir);
        Assert.assertTrue(Files.exists(bag));
        Assert.assertTrue(Files.exists(bag.resolve(BAG_INFO_TXT)));
        Assert.assertTrue(Files.exists(bag.resolve(DATA_DIR)));
        Assert.assertTrue(Files.isDirectory(bag.resolve(DATA_DIR)));
    }

    @Test
    public void testExtractTar() {
        final String serializedBag = expectedDir + ".tar";
        final Path path = target.resolve(group).resolve(serializedBag);
        try {
            final String contentType = Files.probeContentType(path);
            final BagProfile profile = new BagProfile(Files.newInputStream(
                    Paths.get(BEYONDTHEREPOSITORY_JSON)));
            final BagDeserializer deserializer = SerializationSupport.deserializerFor(contentType, profile);
            deserializer.deserialize(path);
        } catch (IOException e) {
            Assert.fail("Unexpected exception:\n" + e.getMessage());
        }

        final Path bag = target.resolve(group).resolve(expectedDir);
        Assert.assertTrue(Files.exists(bag));
        Assert.assertTrue(Files.exists(bag.resolve(BAG_INFO_TXT)));
        Assert.assertTrue(Files.exists(bag.resolve(DATA_DIR)));
        Assert.assertTrue(Files.isDirectory(bag.resolve(DATA_DIR)));
    }

    @Test
    @Ignore
    public void testExtractGZip() {
        final String serializedBag = expectedDir + ".tar.gz";
        final Path path = target.resolve(group).resolve(serializedBag);
        try {
            final String contentType = Files.probeContentType(path);
            final BagProfile profile = new BagProfile(Files.newInputStream(
                    Paths.get(BEYONDTHEREPOSITORY_JSON)));
            final BagDeserializer deserializer = SerializationSupport.deserializerFor(contentType, profile);
            deserializer.deserialize(path);
        } catch (IOException e) {
            Assert.fail("Unexpected exception:\n" + e.getMessage());
        }

        final Path bag = target.resolve(group).resolve(expectedDir);
        Assert.assertTrue(Files.exists(bag));
        Assert.assertTrue(Files.exists(bag.resolve(BAG_INFO_TXT)));
        Assert.assertTrue(Files.exists(bag.resolve(DATA_DIR)));
        Assert.assertTrue(Files.isDirectory(bag.resolve(DATA_DIR)));
    }
}
