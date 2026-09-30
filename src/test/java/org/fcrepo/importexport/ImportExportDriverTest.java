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
package org.fcrepo.importexport;

import java.io.File;

import org.apache.commons.io.FileUtils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the command line entry point.
 *
 * @author barmintor
 * @since 2016-08-31
 */
public class ImportExportDriverTest {

    private final File exportDir = new File("target/driver-test-export");

    @AfterEach
    public void tearDown() {
        FileUtils.deleteQuietly(exportDir);
    }

    @Test
    public void testMainWithInvalidArgsDoesNotThrow() {
        ImportExportDriver.main(new String[]{"-m", "invalid"});
    }

    @Test
    public void testMainRunsExport() {
        // Nothing listens on port 1, so the export task fails and is logged, but the run itself completes
        ImportExportDriver.main(new String[]{"-m", "export",
                "-d", exportDir.getPath(),
                "-r", "http://localhost:1/rest/foo",
                "-R", "http://localhost:1/rest",
                "-T", "1"});
        assertFalse(new File(exportDir, "rest/foo.ttl").exists());
    }
}
