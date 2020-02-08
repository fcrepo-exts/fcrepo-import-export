package org.fcrepo.importexport.common;

public enum BagItDigest {
    MD5(0), SHA1(1), SHA256(2), SHA512(3);

    private final int priority;

    BagItDigest(int priority) {
        this.priority = priority;
    }

    public static BagItDigest fromString(final String algorithm) {
        switch (algorithm.toLowerCase().replace("-", "")) {
            case "md5": return MD5;
            case "sha1": return SHA1;
            case "sha256": return SHA256;
            case "sha512": return SHA512;
            default: throw new RuntimeException("Unsupported bagit algorithm " + algorithm);
        }
    }

    public int getPriority() {
        return priority;
    }
}
