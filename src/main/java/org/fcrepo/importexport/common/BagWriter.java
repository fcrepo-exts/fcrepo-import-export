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

import static org.apache.commons.codec.binary.Hex.encodeHex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Utility to write BagIt bags.
 *
 * @author escowles
 * @since 2016-12-15
 */
public class BagWriter {

    private File bagDir;
    private File dataDir;
    private Set<String> algorithms;

    private Map<String, Map<File, String>> payloadRegistry;
    private Map<String, Map<File, String>> tagFileRegistry;
    private Map<String, Map<String, String>> tagRegistry;

    /**
     * Version of the BagIt specification implemented
     */
    public static String BAGIT_VERSION = "0.97";

    /**
     * Create a new, empty Bag
     * @param bagDir The base directory for the Bag (will be created if it doesn't exist)
     * @param algorithms Set of digest algorithms to use for manifests (e.g., "md5", "sha1", or "sha256")
     */
    public BagWriter(final File bagDir, final Set<String> algorithms) {
        this.bagDir = bagDir;
        this.dataDir = new File(bagDir, "data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        this.algorithms = algorithms;
        payloadRegistry = new HashMap<>();
        tagFileRegistry = new HashMap<>();
        tagRegistry = new HashMap<>();

        final Map<String, String> bagitValues = new HashMap<>();
        bagitValues.put("BagIt-Version", BAGIT_VERSION);
        bagitValues.put("Tag-File-Character-Encoding", "UTF-8");
        tagRegistry.put("bagit.txt", bagitValues);
    }

    /**
     * Get the Bag's root directory
     * @return File object for the directory
     */
    public File getRootDir() {
        return bagDir;
    }

    /**
     * Register checksums of payload (data) files
     * @param algorithm Checksum digest algorithm name (e.g., "SHA-1")
     * @param filemap Map of Files to checksum values
     */
    public void registerChecksums(final String algorithm, final Map<File, String> filemap) {
        if (!algorithms.contains(algorithm)) {
            throw new RuntimeException("Invalid algorithm: " + algorithm);
        }
        payloadRegistry.put(algorithm, filemap);
    }

    /**
     * Add tags (metadata) to the Bag
     * @param key Filename of the tag file (e.g., "bag-info.txt")
     * @param values Map containing field/value pairs
     */
    public void addTags(final String key, final Map<String, String> values) {
        tagRegistry.put(key, values);
    }

    /**
     * Get the current tag (metadata) of the Bag
     * @param key Filename of the tag file (e.g., "bag-info.txt")
     * @return Map of field/value pairs
     */
    public Map<String, String> getTags(final String key) {
        return tagRegistry.get(key);
    }

    /**
     * Write metadata and finalize Bag
     * @throws IOException when an I/O error occurs
     * @throws NoSuchAlgorithmException when an unsupported algorithm is used
     */
    public void write() throws IOException, NoSuchAlgorithmException {
        writeManifests("manifest", payloadRegistry);
        for (final Iterator<String> it = tagRegistry.keySet().iterator(); it.hasNext(); ) {
            writeTagFile(it.next());
        }
        writeManifests("tagmanifest", tagFileRegistry);
    }

    private void writeManifests(final String prefix, final Map<String, Map<File, String>> registry)
            throws IOException {
        for (final Iterator<String> it = algorithms.iterator(); it.hasNext(); ) {
            final String algorithm = it.next();
            final Map<File, String> filemap = registry.get(algorithm);
            if (filemap != null) {
                final File f = new File(bagDir, prefix + "-" + algorithm + ".txt");
                try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(f)))) {
                    for (final Iterator<File> files = filemap.keySet().iterator(); files.hasNext(); ) {
                        final File payload = files.next();
                        out.println(filemap.get(payload) + "  " + bagDir.toPath().relativize(payload.toPath()));
                    }
                }
            }
        }
    }

    private void writeTagFile(final String key) throws IOException, NoSuchAlgorithmException {
        final Map<String, String> values = tagRegistry.get(key);
        final Map<String, String> checksums = new HashMap<>();
        if (values != null) {
            final File f = new File(bagDir, key);

            MessageDigest md5 = null;
            MessageDigest sha1 = null;
            MessageDigest sha256 = null;
            if (algorithms.contains("md5")) {
                md5 = MessageDigest.getInstance("MD5");
            }
            if (algorithms.contains("sha1")) {
                sha1 = MessageDigest.getInstance("SHA-1");
            }
            if (algorithms.contains("sha256")) {
                sha256 = MessageDigest.getInstance("SHA-256");
            }

            try (OutputStream out = new FileOutputStream(f)) {
                for (final Iterator<String> it = values.keySet().iterator(); it.hasNext(); ) {
                    final String field = it.next();
                    final byte[] bytes = (field + " : " + values.get(field) + "\n").getBytes();
                    out.write(bytes);

                    if (md5 != null) {
                        md5.update(bytes);
                    }
                    if (sha1 != null) {
                        sha1.update(bytes);
                    }
                    if (sha256 != null) {
                        sha256.update(bytes);
                    }
                }
            }

            addTagChecksum("md5", f, md5);
            addTagChecksum("sha1", f, sha1);
            addTagChecksum("sha256", f, sha256);
        }
    }

    private void addTagChecksum(final String algorithm, final File f, final MessageDigest digest) {
        if (digest != null) {
            Map<File, String> m = tagFileRegistry.get(algorithm);
            if (m == null) {
                m = new HashMap<>();
                tagFileRegistry.put(algorithm, m);
            }
            m.put(f, new String(encodeHex(digest.digest())));
        }
    }
}
