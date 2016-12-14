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
import static org.junit.Assert.assertNotNull;
import java.io.File;
import java.util.Map;

import org.junit.Test;

/**
 * @author dbernstein
 * @since Dec 14, 2016
 */
public class BagConfigTest {

    @Test
    public void testFromFile() throws Exception {
        final File testFile = new File("src/test/resources/configs/bagit-config.yml");
        final BagConfig config = new BagConfig(testFile);

        final Map<String, String> bagInfo = config.getBagInfo();
        assertNotNull(bagInfo);
        assertNotNull(bagInfo.get(BagConfig.SOURCE_ORGANIZATION_KEY));

        final Map<String, String> aptrustInfo = config.getAPTrustInfo();
        assertNotNull(aptrustInfo);
        assertEquals(aptrustInfo.get(BagConfig.ACCESS_KEY).toUpperCase(), BagConfig.AccessTypes.RESTRICTED.name());

    }

    @Test(expected = RuntimeException.class)
    public void testBadAccessValue() throws Exception {
        final File testFile = new File("src/test/resources/configs/bagit-config-bad-access.yml");
        new BagConfig(testFile);
    }

}
