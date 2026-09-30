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
package org.fcrepo.importexport.patch;

import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that the patched writers registered by {@link RdfWriterHelper} produce output.
 *
 * @author dfield
 */
public class RdfWriterHelperTest {

    private static final String SUBJECT = "http://localhost:8080/rest/1";
    private static final String PREDICATE = "http://purl.org/dc/elements/1.1/title";

    @BeforeClass
    public static void registerWriters() {
        new RdfWriterHelper();
    }

    @Test
    public void testStreamWriters() {
        for (final RDFFormat format : new RDFFormat[]{RDFFormat.TURTLE_BLOCKS, RDFFormat.TURTLE_FLAT,
                RDFFormat.NTRIPLES, RDFFormat.NTRIPLES_UTF8, RDFFormat.NTRIPLES_ASCII}) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final StreamRDF writer = StreamRDFWriter.getWriterStream(out, format);
            writer.start();
            writer.triple(Triple.create(NodeFactory.createURI(SUBJECT), NodeFactory.createURI(PREDICATE),
                    NodeFactory.createLiteral("title")));
            writer.finish();
            assertTrue("No output for " + format, out.toString().contains(SUBJECT));
        }
    }

    @Test
    public void testGraphWriters() {
        final Model model = createDefaultModel();
        model.add(model.createResource(SUBJECT), model.createProperty(PREDICATE), "title");
        for (final RDFFormat format : new RDFFormat[]{RDFFormat.TURTLE_PRETTY, RDFFormat.TURTLE_BLOCKS,
                RDFFormat.TURTLE_FLAT, RDFFormat.RDFXML_PLAIN}) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            RDFDataMgr.write(out, model, format);
            assertTrue("No output for " + format, out.toString().contains(SUBJECT));
        }
    }
}
