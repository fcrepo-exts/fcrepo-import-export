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
    @Ignore
    public void testExtractZip() {
        final String serializedBag = expectedDir + ".zip";
        final Path path = target.resolve(group).resolve(serializedBag);
        try {
            final BagProfile profile = new BagProfile(Files.newInputStream(
                Paths.get("src/main/resources/profiles/beyondtherepository.json")));
            // BagDeserializer.deserialize(path, profile);
        } catch (IOException e) {
            Assert.fail("Unexpected exception:\n" + e.getMessage());
        }

        final Path bag = target.resolve(group).resolve(expectedDir);
        Assert.assertTrue(Files.exists(bag));
        Assert.assertTrue(Files.exists(bag.resolve("bag-info.txt")));
        Assert.assertTrue(Files.exists(bag.resolve("data")));
        Assert.assertTrue(Files.isDirectory(bag.resolve("data")));
    }

    @Test
    public void testExtractZip2() {
        final String serializedBag = expectedDir + ".zip";
        final Path path = target.resolve(group).resolve(serializedBag);
        try {
            final String contentType = Files.probeContentType(path);
            final BagProfile profile = new BagProfile(Files.newInputStream(
                    Paths.get("src/main/resources/profiles/beyondtherepository.json")));
            BagDeserializer deserializer = SerializationSupport.deserializerFor(contentType, profile);
            deserializer.deserialize(path);
        } catch (IOException e) {
            Assert.fail("Unexpected exception:\n" + e.getMessage());
        }

        final Path bag = target.resolve(group).resolve(expectedDir);
        Assert.assertTrue(Files.exists(bag));
        Assert.assertTrue(Files.exists(bag.resolve("bag-info.txt")));
        Assert.assertTrue(Files.exists(bag.resolve("data")));
        Assert.assertTrue(Files.isDirectory(bag.resolve("data")));
    }
}
