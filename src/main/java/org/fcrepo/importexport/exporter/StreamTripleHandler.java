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
package org.fcrepo.importexport.exporter;

import org.apache.jena.graph.Factory;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoOperationFailedException;
import org.fcrepo.client.FcrepoResponse;
import org.fcrepo.importexport.common.Config;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;
import static org.fcrepo.importexport.common.FcrepoConstants.NON_RDF_SOURCE;
import static org.fcrepo.importexport.common.TransferProcess.checkValidResponse;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A class to handle triples from a RDFstream
 * @author whikloj
 */
public class StreamTripleHandler implements StreamRDF {

    // The configuration
    private final Config config;

    // The exporter to use
    private final Exporter exporter;

    // The URI of the current resource
    private Node uri;

    // The output stream to write to
    private OutputStream outputStream = null;

    // The output file to write to
    protected File file;

    // The other resources to export found in the triples
    private List<Node> exports = new ArrayList<>();

    // The predicates to include in the export as a list of nodes
    private final List<Node> predicates = new ArrayList<>();

    // The RDF language to write in (currently only NTRIPLES)
    private final Lang rdfLanguage;

    // The FcrepoClient to perform head requests to exclude binaries if necessary.
    private final FcrepoClient client;

    private static final Logger LOGGER = getLogger(StreamTripleHandler.class);

    // The URI for a binary resource rdf:type
    private static final URI binaryURI = URI.create(NON_RDF_SOURCE.getURI());

    /**
     * Constructor
     * @param config the configuration
     * @param transferProcess the exporter
     * @param client the FcrepoClient
     */
    public StreamTripleHandler (
            final Config config,
            final Exporter transferProcess,
            final FcrepoClient client
    ) {
        this.config = config;
        this.rdfLanguage = contentTypeToLang(config.getRdfLanguage());
        this.exporter = transferProcess;
        this.client = client;
        this.predicates.addAll(Arrays.stream(config.getPredicates()).map(ResourceFactory::createProperty)
                .map(Property::asNode).collect(Collectors.toList()));
    }

    /**
     * Set the resource URI.
     * @param resource the resource URI
     * @return this
     */
    public StreamTripleHandler setResource(final URI resource) {
        this.uri = ResourceFactory.createResource(resource.toString()).asNode();
        return this;
    }

    /**
     * Set the file to write to.
     * @param file the file to write to
     * @return this
     */
    public StreamTripleHandler setFile(final File file) {
        this.file = file;
        return this;
    }

    @Override
    public void start() {
        LOGGER.trace("Starting stream triple handler");
        exports = new ArrayList<>();
        if (file == null) {
            LOGGER.error("No file set for output stream");
            return;
        }
        if (uri == null) {
            LOGGER.error("No resource URI set");
            return;
        }
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(file));
        } catch (FileNotFoundException e) {
            LOGGER.error("Error creating output stream: {}", e.getMessage());
        }
    }

    @Override
    public void triple(final Triple triple) {
        LOGGER.trace("Triple: {}", triple);
        if (triple.subjectMatches(uri)) {
            LOGGER.debug("Found triple with subject: {}", uri);
            if (predicates.stream().anyMatch(triple::predicateMatches)) {
                LOGGER.trace("Capturing object resource {} with predicate {}", uri, triple.getPredicate());
                if (!config.isIncludeBinaries()) {
                    try {
                        if (isBinary(URI.create(triple.getObject().getURI()))) {
                            LOGGER.debug("Skipping binary resource: {}", triple.getObject());
                            return;
                        }
                    } catch (IOException | FcrepoOperationFailedException e) {
                        LOGGER.error("Error checking if resource is binary: {}", e.getMessage());
                    }
                }
                exports.add(triple.getObject());
            }
        } else if (triple.objectMatches(uri)) {
            LOGGER.debug("Found triple with object: {}", uri);
            if (config.retrieveInbound()) {
                LOGGER.trace("Capturing inbound reference: {}", uri);
                exports.add(triple.getSubject());
            } else {
                LOGGER.debug("Skipping inbound reference: {}", uri);
                return;
            }
        }
        final Graph graph = Factory.createDefaultGraph();
        graph.add(triple);
        RDFDataMgr.write(outputStream, graph, rdfLanguage);
    }

    @Override
    public void quad(final Quad quad) {
        LOGGER.trace("Quad: {}", quad);
        this.triple(quad.asTriple());
    }

    @Override
    public void base(final String s) {
        LOGGER.trace("Base: {}", s);
        // no-op
    }

    @Override
    public void prefix(final String s, final String s1) {
        LOGGER.trace("Prefix: {} {}", s, s1);
        // no-op
    }

    @Override
    public void finish() {
        LOGGER.debug("Finishing stream triple handler");
        try {
            if (outputStream != null) {
                outputStream.close();
                if (this.file != null && this.file.exists()) {
                    exporter.generateChecksums(this.file);
                }
            }
            if (!exports.isEmpty()) {
                LOGGER.info("Exporting {} resources linked to {}", exports.size(), uri);
                for (Node export : exports) {
                    exporter.export(URI.create(export.getURI()));
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error closing output stream: {}", e.getMessage());
        } finally {
            // Reset the output stream and file
            this.outputStream = null;
            this.file = null;
            this.uri = null;
        }
    }

    /**
     * Check if the resource at obj is a binary.
     * @param obj the URI of the resource
     * @return true if the resource is a binary
     * @throws IOException if there is an error performing the HEAD request
     * @throws FcrepoOperationFailedException if there is an error performing the HEAD request
     */
    private boolean isBinary(final URI obj) throws IOException, FcrepoOperationFailedException {
        try (final FcrepoResponse resp = client.head(URI.create(obj.toString())).disableRedirects().perform()) {
            checkValidResponse(resp, URI.create(obj.toString()), config.getUsername());
            final List<URI> linkHeaders = resp.getLinkHeaders("type");
            return linkHeaders.contains(binaryURI);
        }
    }
}
