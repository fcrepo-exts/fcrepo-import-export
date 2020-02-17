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
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Support class to retrieve {@link BagDeserializer}s from a mime type
 *
 * @author mikejritter
 * @since 2020-02-11
 */
public class SerializationSupport {

    private static final Logger logger = LoggerFactory.getLogger(SerializationSupport.class);

    // zip
    private static final String APPLICATION_ZIP = "application/zip";

    // tar + gtar
    private static final String APPLICATION_TAR = "application/tar";
    private static final String APPLICATION_GTAR = "application/gtar";
    private static final String APPLICATION_X_TAR = "application/x-tar";
    private static final String APPLICATION_X_GTAR = "application/x-gtar";

    // gzip
    private static final String APPLICATION_GZIP = "application/gzip";
    private static final String APPLICATION_X_GZIP = "application/x-gzip";
    private static final String APPLICATION_X_COMPRESSED_TAR = "application/x-compressed-tar";

    // 7zip
    // private static final String APPLICATION_X_7Z_COMPRESSED = "application/x-7zip-compressed";

    public static final Set<String> ZIP_TYPES = Collections.singleton(APPLICATION_ZIP);
    public static final Set<String> TAR_TYPES = new HashSet<>(Arrays.asList(APPLICATION_TAR, APPLICATION_X_TAR,
                                                                            APPLICATION_GTAR, APPLICATION_X_GTAR));
    public static final Set<String> GZIP_TYPES = new HashSet<>(Arrays.asList(APPLICATION_GZIP, APPLICATION_X_GTAR,
                                                                             APPLICATION_X_COMPRESSED_TAR));
    // public static final Set<String> SEVEN_ZIP_TYPES = Collections.singleton("application/x-7zip-compressed");

    /**
     * The commonTypeMap acts as a way to coerce various types onto a single format. E.g. handing application/gtar and
     * application/tar will go through the same class, so we map application/gtar to application/tar.
     */
    private static Map<String, String> commonTypeMap = initCommonTypeMapping();

    private SerializationSupport() {
    }

    /**
     * Just a way to instantiate the {@code commonTypeMap}
     *
     * @return the map of supported application types
     */
    private static Map<String, String> initCommonTypeMapping() {
        commonTypeMap = new HashMap<>();
        commonTypeMap.put(APPLICATION_ZIP, APPLICATION_ZIP);

        commonTypeMap.put(APPLICATION_TAR, APPLICATION_TAR);
        commonTypeMap.put(APPLICATION_GTAR, APPLICATION_TAR);
        commonTypeMap.put(APPLICATION_X_TAR, APPLICATION_X_TAR);
        commonTypeMap.put(APPLICATION_X_GTAR, APPLICATION_X_TAR);

        commonTypeMap.put(APPLICATION_GZIP, APPLICATION_GZIP);
        commonTypeMap.put(APPLICATION_X_GZIP, APPLICATION_GZIP);
        commonTypeMap.put(APPLICATION_X_COMPRESSED_TAR, APPLICATION_GZIP);
        return commonTypeMap;
    }

    /**
     * Get a {@link BagDeserializer} for a given content type. Currently supported are:
     * zip ({@link SerializationSupport#ZIP_TYPES}) - {@link ZipBagDeserializer}
     * tar ({@link SerializationSupport#TAR_TYPES}) - {@link TarBagDeserializer}
     * tar+gz ({@link SerializationSupport#GZIP_TYPES}) - {@link GZipBagDeserializer}
     *
     * @param serializedBag the Bag (still serialized) to get a {@link BagDeserializer} for
     * @param profile the {@link BagProfile} to ensure that the content type is allowed
     * @return the {@link BagDeserializer}
     * @throws UnsupportedOperationException if the content type is not supported
     * @throws RuntimeException if the {@link BagProfile} does not allow serialization
     */
    public static BagDeserializer deserializerFor(final Path serializedBag, final BagProfile profile) {
        final Tika tika = new Tika();
        final String contentType;

        try {
            // use a less strict approach to handling content types through the commonTypeMap
            final String detectedType = tika.detect(serializedBag);
            contentType = commonTypeMap.getOrDefault(detectedType, detectedType);
            logger.debug("{}: {}", serializedBag, contentType);
        } catch (IOException e) {
            logger.error("Unable to get content type for {}", serializedBag);
            throw new RuntimeException(e);
        }

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
