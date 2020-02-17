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

public abstract class BagProfileConstants {
    public static final String ALLOW_FETCH_TXT = "Allow-Fetch.txt";
    public static final String SERIALIZATION = "Serialization";
    public static final String ACCEPT_BAGIT_VERSION = "Accept-BagIt-Version";
    public static final String ACCEPT_SERIALIZATION = "Accept-Serialization";
    public static final String TAG_FILES_ALLOWED = "Tag-Files-Allowed";
    public static final String TAG_FILES_REQUIRED = "Tag-Files-Required";
    public static final String MANIFESTS_ALLOWED = "Manifests-Allowed";
    public static final String TAG_MANIFESTS_ALLOWED = "Tag-Manifests-Allowed";
    public static final String MANIFESTS_REQUIRED = "Manifests-Required";
    public static final String TAG_MANIFESTS_REQUIRED = "Tag-Manifests-Required";
    public static final String BAGIT_PROFILE_INFO = "BagIt-Profile-Info";
    public static final String OTHER_INFO = "Other-Info";

    // fields within Bag-Profile-Info
    public static final String PROFILE_VERSION = "Version";
    public static final String BAGIT_PROFILE_VERSION = "BagIt-Profile-Version";
    public static final String BAGIT_PROFILE_IDENTIFIER = "BagIt-Profile-Identifier";

    public static final String BAGIT_MD5 = "md5";
    public static final String BAGIT_SHA1 = "sha1";
    public static final String BAGIT_SHA_256 = "sha256";
}
