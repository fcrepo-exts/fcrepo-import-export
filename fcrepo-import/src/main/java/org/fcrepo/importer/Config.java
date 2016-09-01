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
package org.fcrepo.importer;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author awoods
 * @since 2016-08-29
 */
public class Config implements org.fcrepo.importexport.Config {

    private String mode;

    /**
     * This method returns true if the configuration is set for 'import'
     *
     * @return true if import config
     */
    public boolean isImport() {
        return mode.equalsIgnoreCase("import");
    }

    /**
     * This method returns true if the configuration is set for 'export'
     *
     * @return true if export config
     */
    public boolean isExport() {
        return !isImport();
    }

    /**
     * This method sets the import/export mode
     *
     * @param mode of this configuration
     */
    public void setMode(final String mode) {
        this.mode = mode;
    }

	@Override
	public void setResource(String resource) throws URISyntaxException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setResource(URI resource) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public URI getResource() {
		// TODO Auto-generated method stub
		return null;
	}
}
