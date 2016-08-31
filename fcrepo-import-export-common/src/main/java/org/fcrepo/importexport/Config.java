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

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author awoods
 * @author escowles
 * @since 2016-08-29
 */
public interface Config {

    /**
     * This method returns true if the configuration is set for 'import'
     *
     * @return true if import config
     */
    public boolean isImport();

    /**
     * This method returns true if the configuration is set for 'export'
     *
     * @return true if export config
     */
    public boolean isExport();

    /**
     * This method sets the import/export mode
     *
     * @param mode of this configuration
     */
    public void setMode(final String mode);

    /**
     * Sets the URI of the resource to import/export
     */
    public void setResource(final String resource) throws URISyntaxException;

    /**
     * Sets the URI of the resource to import/export
     */
    public void setResource(final URI resource);

    /**
     * Gets the URI of the resource to import/export
     */
    public URI getResource();

}
