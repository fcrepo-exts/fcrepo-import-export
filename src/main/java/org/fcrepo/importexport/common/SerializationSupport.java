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

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class SerializationSupport {

    public static final Set<String> ZIP_TYPES = Collections.singleton("application/zip");
    public static final Set<String> TAR_TYPES = new HashSet<>(Arrays.asList("application/tar", "application/x-tar"));
    public static final Set<String> GZIP_TYPES = new HashSet<>(Arrays.asList("application/gzip", "application/x-gzip"));
    public static final Set<String> SEVEN_ZIP_TYPES = Collections.singleton("application/x-7zip-compressed");

    private SerializationSupport() {
    }

    /**
     *
     * @param contentType
     * @param profile
     * @return
     */
    public static BagDeserializer deserializerFor(final String contentType, final BagProfile profile) {
        if (profile.getAcceptedSerializations().contains(contentType)) {
            if (ZIP_TYPES.contains(contentType)) {
                return new ZipBagDeserializer();
            } else if (TAR_TYPES.contains(contentType)) {
                return new TarBagDeserializer();
            } else if (GZIP_TYPES.contains(contentType)) {
                return new GZipBagDeserializer();
            } else {
                throw new UnsupportedOperationException("Unsupported content type " + contentType);
            }
        }

        throw new RuntimeException("BagProfile does not allow " + contentType + ". Accepted serializations are:\n" +
                StringUtils.join(profile.getAcceptedSerializations(), ", "));
    }


}
