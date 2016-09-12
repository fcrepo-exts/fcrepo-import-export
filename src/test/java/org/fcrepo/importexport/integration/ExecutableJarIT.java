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
package org.fcrepo.importexport.integration;

import static javax.ws.rs.core.Response.Status.CREATED;
import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.ArgParser;
import org.fcrepo.importexport.common.TransferProcess;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * @author awoods
 * @since 2016-09-01
 */

public class ExecutableJarIT extends AbstractResourceIT {

    private static final Logger logger = getLogger(ExecutableJarIT.class);

    private URI url;

    private static final String EXECUTABLE = System.getProperty("fcrepo.executable.jar");
    private static final String TARGET_DIR = System.getProperty("project.build.directory");

    private static final int TIMEOUT_SECONDS = 1000;

    public ExecutableJarIT() throws Exception {
        super();
        client = FcrepoClient.client().credentials("fedoraAdmin", "password").authScope("localhost").build();
    }

    @Before
    public void before() {
        url = URI.create(serverAddress + UUID.randomUUID());
        assertNotNull(EXECUTABLE);
        assertTrue(EXECUTABLE + " doesn't exist!", new File(EXECUTABLE).exists());
        assertNotNull(TARGET_DIR);
    }

    @Test
    public void testJarSanity() throws IOException, InterruptedException {
        // Run the executable jar with no arguments
        final Process process = startJarProcess();

        // Verify it ran
        assertTrue("Process did not exit before timeout!", process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("Did not exit with success status!", 0, process.exitValue());
    }

    @Test
    public void testJarExport() throws Exception {
        // Create a repository resource
        final FcrepoResponse response = create();
        assertEquals(CREATED.getStatusCode(), response.getStatusCode());
        assertEquals(url, response.getLocation());

        // Run an export process
        final Process process = startJarProcess("-m", "export",
                "-d", TARGET_DIR,
                "-r", url.toString(),
                "-u", "fedoraAdmin:password");


        // Verify
        assertTrue("Process did not exit before timeout!", process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("Did not exit with success status!", 0, process.exitValue());

        assertTrue(new File(TARGET_DIR, url.getPath() + ArgParser.DEFAULT_RDF_EXT).exists());
    }

    @Test
    public void testConfigFileExport() throws Exception {
        // Create a repository resource
        final FcrepoResponse response = create();
        assertEquals(CREATED.getStatusCode(), response.getStatusCode());
        assertEquals(url, response.getLocation());

        // Create test config file
        final File configFile = File.createTempFile("config-test", ".txt");
        final FileWriter writer = new FileWriter(configFile);
        writer.append("-d\n");
        writer.append(TARGET_DIR);
        writer.append("\n");
        writer.append("-m\n");
        writer.append("export\n");
        writer.append("-r\n");
        writer.append(url.toString());
        writer.append("\n");
        writer.flush();

        // Run an export process
        final Process process = startJarProcess("-c", configFile.getAbsolutePath(), "-u", "fedoraAdmin:password");

        // Verify
        assertTrue("Process did not exit before timeout!", process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals("Did not exit with success status!", 0, process.exitValue());

        assertTrue(new File(TARGET_DIR, TransferProcess.encodePath(url.getPath() + ArgParser.DEFAULT_RDF_EXT))
                .exists());
        assertTrue(new File(System.getProperty("java.io.tmpdir"), ArgParser.CONFIG_FILE_NAME).exists());
    }

    @Test
    public void testExportBinaryAndRDFToSamePath() throws Exception {
        final String content = "Hello World 😀";

        // create a binary resource
        final FcrepoResponse response = createUTF8PlaintextBinary(content);
        assertEquals(CREATED.getStatusCode(), response.getStatusCode());
        assertEquals(url, response.getLocation());


        // Run an export process
        final Process process = startJarProcess("-m", "export",
                "-d", TARGET_DIR,
                "-b", TARGET_DIR,
                "-r", url.toString(),
                "-u", "fedoraAdmin:password");

        // Verify
        assertTrue("Process did not exit before timeout!", process.waitFor(1000, TimeUnit.SECONDS));
        assertEquals("Did not exit with success status!", 0, process.exitValue());

        final List<URI> describedByHeaders = response.getLinkHeaders("describedby");
        assertFalse("Fedora should have given us at least one describedby header!", describedByHeaders.isEmpty());
        describedByHeaders.forEach(uri -> assertTrue("RDF for exported " + uri + " not found!",
                        new File(TARGET_DIR, TransferProcess.encodePath(uri.getPath())
                                + ArgParser.DEFAULT_RDF_EXT).exists()));
        final File exportedBinary
                = new File(TARGET_DIR, TransferProcess.encodePath(url.getPath()) + BINARY_EXTENSION);
        assertTrue(exportedBinary.exists());
        assertEquals("Content was corrupted on export!", content, FileUtils.readFileToString(exportedBinary, "UTF-8"));
    }

    private Process startJarProcess(final String ... args) throws IOException {
        final String[] fullArgs = new String[3 + args.length];
        fullArgs[0] = "java";
        fullArgs[1] = "-jar";
        fullArgs[2] = EXECUTABLE;
        System.arraycopy(args, 0, fullArgs, 3, args.length);
        final ProcessBuilder builder = new ProcessBuilder(fullArgs);

        builder.directory(new File(TARGET_DIR));
        final Process process = builder.start();
        logger.debug("Output of jar run: {}", IOUtils.toString(process.getInputStream()));
        return process;
    }

    private FcrepoResponse createUTF8PlaintextBinary(final String content) throws FcrepoOperationFailedException {
        logger.debug("Request ------: {}", url);
        try {
            return client.put(url).body(new ByteArrayInputStream(content.getBytes("UTF-8")), "text/plain").perform();
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private FcrepoResponse create() throws FcrepoOperationFailedException {
        logger.debug("Request ------: {}", url);
        return client.put(url).perform();
    }

}
