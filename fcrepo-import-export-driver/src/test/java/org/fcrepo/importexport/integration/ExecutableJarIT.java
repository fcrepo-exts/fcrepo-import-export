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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.exporter.ArgParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author awoods
 * @since 2016-09-01
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/spring-test/test-container.xml")
public class ExecutableJarIT extends AbstractResourceIT {

    private static final Logger logger = getLogger(ExecutableJarIT.class);

    private URI url;

    private static final String EXECUTABLE = System.getProperty("fcrepo.executable.jar");
    private static final String TARGET_DIR = System.getProperty("project.build.directory");

    public ExecutableJarIT() throws Exception {
        super();
        client = FcrepoClient.client().build();
    }

    @Before
    public void before() {
        url = URI.create(serverAddress + UUID.randomUUID());
        assertNotNull(EXECUTABLE);
        assertNotNull(TARGET_DIR);
    }

    @Test
    public void testJarSanity() throws IOException, InterruptedException {
        // Run the executable jar with no arguments
        final ProcessBuilder builder = new ProcessBuilder("java", "-jar", EXECUTABLE);
        builder.directory(new File(TARGET_DIR));
        final Process process = builder.start();
        logger.debug("Output of jar run: {}", IOUtils.toString(process.getInputStream()));

        // Verify it ran
        assertTrue("Process did not exit before timeout!", process.waitFor(1000, TimeUnit.SECONDS));
        assertEquals("Did not exit with success status!", 0, process.exitValue());
    }

    @Test
    public void testJarExport() throws Exception {
        // Create a repository resource
        final FcrepoResponse response = create();
        assertEquals(CREATED.getStatusCode(), response.getStatusCode());
        assertEquals(url, response.getLocation());

        // Run an export process
        final ProcessBuilder builder = new ProcessBuilder("java", "-jar", EXECUTABLE,
                "-m", "export",
                "-d", TARGET_DIR,
                "-r", url.toString());

        builder.directory(new File(TARGET_DIR));
        final Process process = builder.start();
        logger.debug("Output of jar run: {}", IOUtils.toString(process.getInputStream()));

        // Verify
        assertTrue("Process did not exit before timeout!", process.waitFor(1000, TimeUnit.SECONDS));
        assertEquals("Did not exit with success status!", 0, process.exitValue());

        assertTrue(new File(TARGET_DIR, url.getPath() + ArgParser.DEFAULT_RDF_EXT).exists());
    }

    private FcrepoResponse create() throws FcrepoOperationFailedException {
        logger.debug("Request ------: {}", url);
        return client.put(url).perform();
    }

}
