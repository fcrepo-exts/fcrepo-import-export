package org.fcrepo.importexport.common;


import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

public class BagDeserializerTest {

    @Test
    public void testSplit() {
        final Path path = Paths.get("src/test/resources/sample/compress/example.zip");
        try {
            final BagProfile profile = new BagProfile(Files.newInputStream(Paths.get("src/main/resources/profiles/perseids.json")));
            BagDeserializer.deserialize(path, profile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
