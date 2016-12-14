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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import com.esotericsoftware.yamlbeans.YamlReader;

/**
 * A convenience class for parsing and storing bagit-config.yml information. The bagit-config.yml represents
 * user-defined properties to be included in the bag-info.txt.
 *
 * @author dbernstein
 * @since Dec 14, 2016
 */
public class BagConfig {

    private static final String BAG_INFO_KEY = "bag-info.txt";

    private static final String APTRUST_INFO_KEY = "aptrust-info.txt";

    public static final String SOURCE_ORGANIZATION_KEY = "Source-Organization";

    public static final String ORGANIZATION_ADDRESS_KEY = "Organization-Address";

    public static final String CONTACT_NAME_KEY = "Contact-Name";

    public static final String CONTACT_PHONE_KEY = "Contact-Phone";

    public static final String CONTACT_EMAIL_KEY = "Contact-Email";

    public static final String EXTERNAL_DESCRIPTION_KEY = "External-Description";

    public static final String EXTERNAL_IDENTIFIER_KEY = "External-Identifier";

    public static final String INTERNAL_SENDER_DESCRIPTION_KEY = "Internal-Sender-Description";

    public static final String INTERNAL_SENDER_IDENTIFIER_KEY = "Internal-Sender-Identifier";

    public static final String BAG_GROUP_IDENTIFIER = "Bag-Group-Identifier";

    public static final String TITLE_KEY = "Title";

    public static final String ACCESS_KEY = "Access";

    private Map<String, Map<String, String>> map;

    /**
     * Default constructor
     *
     * @param bagConfigFile a bagit config yaml file (see src/test/resources/bagit-config.yml)
     */
    @SuppressWarnings("unchecked")
    public BagConfig(final File bagConfigFile) {
        final String bagConfigFilePath = bagConfigFile.getAbsolutePath();

        YamlReader reader = null;
        try {
            reader = new YamlReader(new FileReader(bagConfigFile));
            map = (Map<String, Map<String, String>>) reader.read();
            if (getBagInfo() == null) {
                throw new RuntimeException("The " + BAG_INFO_KEY + " key is not present in the " + bagConfigFilePath);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("The specified bag config file does not exist: " + bagConfigFile
                    .getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException("The specified bag config file could not be parsed: " + e.getMessage(), e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Returns a map of bag info properties.
     *
     * @return a map of bag info properties
     */
    public Map<String, String> getBagInfo() {
        return this.map.get(BAG_INFO_KEY);
    }

    /**
     * Returns a map of aptrust info properties.
     *
     * @return a map of aptrust info properties
     */
    public Map<String, String> getAPTrustInfo() {
        return this.map.get(APTRUST_INFO_KEY);
    }

}
