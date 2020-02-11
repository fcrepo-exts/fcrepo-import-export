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
 * Support class to retrieve {@link BagDeserializer}s from a mime type
 *
 * @author mikejritter
 * @since 2020-02-11
 */
public class SerializationSupport {

    public static final Set<String> ZIP_TYPES = Collections.singleton("application/zip");
    public static final Set<String> TAR_TYPES = new HashSet<>(Arrays.asList("application/tar", "application/x-tar"));
    public static final Set<String> GZIP_TYPES = new HashSet<>(Arrays.asList("application/gzip", "application/x-gzip"));
    public static final Set<String> SEVEN_ZIP_TYPES = Collections.singleton("application/x-7zip-compressed");

    private SerializationSupport() {
    }

    /**
     * Get a {@link BagDeserializer} for a given content type. Currently supported are:
     * zip - {@link ZipBagDeserializer}
     * tar - {@link TarBagDeserializer}
     * tar+gz - {@link GZipBagDeserializer}
     *
     * @param contentType the content type to get a {@link BagDeserializer} for
     * @param profile the {@link BagProfile} to ensure that the content type is allowed
     * @return the {@link BagDeserializer}
     * @throws UnsupportedOperationException if the content type is not supported
     * @throws RuntimeException if the {@link BagProfile} does not allow serialization
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
