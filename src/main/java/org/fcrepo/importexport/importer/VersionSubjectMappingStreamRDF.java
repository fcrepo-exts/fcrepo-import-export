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
package org.fcrepo.importexport.importer;

import static org.apache.jena.graph.NodeFactory.createURI;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_VERSIONS_PATH;

import java.net.URI;

import org.apache.jena.graph.Node;

/**
 * StreamRDF implementation that maps URIs to a specified destination URI and strips out version paths.
 *
 * @author escowles
 * @author bbpennel
 * 
 */
public class VersionSubjectMappingStreamRDF extends SubjectMappingStreamRDF {

    /**
     * Create a version subject-mapping RDF stream
     * @param sourceURI the source URI to map triples from
     * @param destinationURI the destination URI to map triples to
     */
    public VersionSubjectMappingStreamRDF(final URI sourceURI, final URI destinationURI) {
        super(sourceURI, destinationURI);
    }

    @Override
    protected Node rebase(final Node node) {
        if (node.isURI()) {
            // Replace uri base and strip out versions path
            String rebasedNodeUri = node.getURI();
            if (rebasedNodeUri.contains(FCR_VERSIONS_PATH)) {
                rebasedNodeUri = rebasedNodeUri.replaceFirst("/fcr:versions/[^/]+", "");
            }
            if (sourceURI != null && destinationURI != null
                    && node.getURI().startsWith(sourceURI)) {
                rebasedNodeUri = rebasedNodeUri.replaceFirst(sourceURI, destinationURI);
            }
            if (!rebasedNodeUri.equals(node.getURI())) {
                return createURI(rebasedNodeUri);
            }
        }

        return node;
    }
}
