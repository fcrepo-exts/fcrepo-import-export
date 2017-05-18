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

import static java.net.URI.create;
import static org.fcrepo.importexport.common.TransferProcess.fileForURI;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URI;

import org.junit.Test;

/**
 * @author escowles
 * @since 2016-12-14
 */
public class TransferProcessTest {

    private File dir = new File("/export/dir/");
    private String ext = ".ttl";

    @Test
    public void testMappingNull() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, null, null, dir, ext));
    }

    @Test
    public void testMappingBase() throws Exception {
        final URI uri = create("http://localhost:8080/fedora/rest/foo");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, "/rest", "/fedora/rest", dir, ext));
    }

    @Test
    public void testMappingPath() throws Exception {
        final URI uri = create("http://localhost:8888/rest/prod/foo");
        assertEquals(new File(dir, "rest/dev/foo" + ext), fileForURI(uri, "/rest/dev", "/rest/prod", dir, ext));
    }

    @Test
    public void testMappingRest() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo");
        assertEquals(new File(dir, "rest/foo" + ext), fileForURI(uri, "/rest", "/rest", dir, ext));
    }

    @Test
    public void testMappingRestless() throws Exception {
        final URI uri = create("http://localhost:8080/rest/foo");
        assertEquals(new File(dir, "foo" + ext), fileForURI(uri, "/", "/rest/", dir, ext));
    }

    @Test
    public void testMappingRestless2() throws Exception {
        final URI uri = create("http://localhost:8080/foo");
        assertEquals( new File(dir, "rest/foo" + ext), fileForURI(uri, "/rest/", "/", dir, ext));
    }
}
