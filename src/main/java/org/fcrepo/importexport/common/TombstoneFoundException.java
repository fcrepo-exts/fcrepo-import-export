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

import java.net.URI;

/**
 * Exception thrown when a tombstone is found during export.
 * @author whikloj
 */
public class TombstoneFoundException extends RuntimeException {
  /**
   * Default constructor: provides a default message.
   * @param uri the URI of the requested resource
   */
  public TombstoneFoundException(final URI uri) {
    super("Tombstone found at " + uri);
  }
}
