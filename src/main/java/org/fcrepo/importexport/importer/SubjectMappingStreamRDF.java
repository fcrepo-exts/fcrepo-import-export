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

import static org.apache.jena.graph.Factory.createDefaultGraph;
import static org.apache.jena.rdf.model.ModelFactory.createModelForGraph;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.slf4j.LoggerFactory.getLogger;

import java.net.URI;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;

import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.lang.SinkTriplesToGraph;
import org.apache.jena.riot.system.StreamRDFBase;

/**
 * StreamRDF implementation that maps URIs to a specified base URI.
 *
 * @author escowles
 * @since 2016-09-08
 */
public class SubjectMappingStreamRDF extends StreamRDFBase {
    private static final Logger logger = getLogger(SubjectMappingStreamRDF.class);

    private final String sourceURI;
    private final String baseURI;
    private final Graph graph;
    private final Sink<Triple> sink;

    /**
     * Create a subject-mapping RDF stream
     * @param sourceURI the source URI to map triples from
     * @param baseURI the base URI to map triples to
     */
    public SubjectMappingStreamRDF(final URI sourceURI, final URI baseURI) {
        this.sourceURI = sourceURI.toString();
        this.baseURI = baseURI.toString();
        this.graph = createDefaultGraph();
        this.sink = new SinkTriplesToGraph(true, graph);
    }

    @Override
    public void triple(final Triple t) {
        sink.send(Triple.create(rebase(t.getSubject()), t.getPredicate(), rebase(t.getObject())));
    }

    private Node rebase(final Node node) {
        if (node.isURI() && node.getURI().startsWith(sourceURI)) {
            return createURI(node.getURI().replaceFirst(sourceURI, baseURI));
        }

        return node;
    }

    @Override
    public void finish() {
        sink.flush();
        sink.close();
    }

    /**
     * Get the mapped triples as a model
     * @return A model representing the triples sent to this stream
     */
    public Model getModel() {
        return createModelForGraph(graph);
    }
}
