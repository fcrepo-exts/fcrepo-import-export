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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a file that contains a single URI per line
 *
 * @author pwinckles
 */
public final class ResourceFileParser {

    private ResourceFileParser() {
        // static class
    }

    /**
     * Parses a file that contains a single URI per line and returns a list of all of the URIs
     *
     * @param path path to the file
     * @return list of URIs
     */
    public static List<URI> parse(final Path path) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(path)))) {
            final List<URI> uris = new ArrayList<>();
            while (reader.ready()) {
                uris.add(URI.create(reader.readLine()));
            }
            return uris;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
