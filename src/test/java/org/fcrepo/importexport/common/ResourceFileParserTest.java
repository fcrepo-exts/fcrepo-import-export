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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author dfield
 */
public class ResourceFileParserTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testParse() throws IOException {
        final File file = tmp.newFile("resources.txt");
        Files.write(file.toPath(), Arrays.asList("http://localhost:8080/rest/a", "http://localhost:8080/rest/b"));

        final List<URI> uris = ResourceFileParser.parse(file.toPath());

        assertEquals(Arrays.asList(URI.create("http://localhost:8080/rest/a"),
                URI.create("http://localhost:8080/rest/b")), uris);
    }

    @Test
    public void testParseMissingFile() {
        final File missing = new File(tmp.getRoot(), "missing.txt");
        assertThrows(UncheckedIOException.class, () -> ResourceFileParser.parse(missing.toPath()));
    }
}
