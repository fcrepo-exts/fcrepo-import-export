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
import java.net.URI;

import org.junit.Test;

/**
 * @author escowles
 * @since 2017-05-26
 */
public class UriUtilsTest {

    private final URI withSlash = URI.create("http://localhost:8080/rest/");
    private final URI withOutSlash = URI.create("http://localhost:8080/rest/");

    @Test
    public void testWithSlash() throws Exception {
        assertEquals(withSlash, UriUtils.withSlash(withSlash));
        assertEquals(withSlash, UriUtils.withSlash(withOutSlash));
    }

    @Test
    public void testWithoutSlash() throws Exception {
        assertEquals(withOutSlash, UriUtils.withSlash(withSlash));
        assertEquals(withOutSlash, UriUtils.withSlash(withOutSlash));
    }
}
