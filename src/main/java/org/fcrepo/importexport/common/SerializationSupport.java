package org.fcrepo.importexport.common;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SerializationSupport {

    public static final Set<String> ZIP_TYPES = Collections.singleton("application/zip");
    public static final Set<String> TAR_TYPES = new HashSet<>(Arrays.asList("application/tar", "application/x-tar"));
    public static final Set<String> GZIP_TYPES = new HashSet<>(Arrays.asList("application/gzip", "application/x-gzip"));
    public static final Set<String> SEVEN_ZIP_TYPES = Collections.singleton("application/x-7zip-compressed");

    private SerializationSupport() {
    }


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
