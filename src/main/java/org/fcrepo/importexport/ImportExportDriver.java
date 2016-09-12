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

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

import org.fcrepo.importexport.common.TransferProcess;

/**
 * @author ruebot
 * @since 2016-08-29
 */
public class ImportExportDriver {

    private static final Logger logger = getLogger(ImportExportDriver.class);

    private ImportExportDriver() {
        // Prevent public instantiation
    }

    /**
     * The main entry point
     *
     * @param args from the command line
     */
    public static void main(final String[] args) {
        final ImportExportDriver driver = new ImportExportDriver();

        try {
            driver.run(args);

        } catch (final Exception e) {
            logger.error("Error performing import/export: {}", e.getMessage());
            logger.debug("Stacktrace: ", e);
        }
    }

    private void run(final String[] args) {
        final ArgParser parser = new ArgParser();
        final TransferProcess processor = parser.parse(args);

        processor.run();
    }

}
