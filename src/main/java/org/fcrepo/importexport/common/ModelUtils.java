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

import static org.apache.jena.riot.RDFLanguages.contentTypeToLang;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.fcrepo.importexport.importer.SubjectMappingStreamRDF;
import org.fcrepo.importexport.importer.VersionSubjectMappingStreamRDF;

/**
 * Utilities for manipulating rdf models
 *
 * @author bbpennel
 *
 */
public abstract class ModelUtils {

    /**
     * Parses an input stream of RDF in the configured RDF language into a model, remapping subjects to the configured
     * destination URL.  This includes removal of version path components if configured.
     *
     * @param in RDF input stream
     * @param config config
     * @return parsed model
     * @throws IOException Thrown if the stream cannot be parsed
     */
    public static Model mapRdfStreamToNonversionedSubjects(final InputStream in, final Config config)
            throws IOException {
        final SubjectMappingStreamRDF mapper;
        if (config.includeVersions()) {
            mapper = new VersionSubjectMappingStreamRDF(config.getSource(), config.getDestination());
        } else {
            mapper = new SubjectMappingStreamRDF(config.getSource(), config.getDestination());
        }

        try (final InputStream in2 = in) {
            RDFDataMgr.parse(mapper, in2, contentTypeToLang(config.getRdfLanguage()));
        }
        return mapper.getModel();
    }

    /**
     * Parses an input stream of RDF in the configured RDF language into a model, remapping subjects to the configured
     * destination URL.
     *
     * @param in RDF input stream
     * @param config config
     * @return parsed model
     * @throws IOException Thrown if the stream cannot be parsed
     */
    public static Model mapRdfStream(final InputStream in, final Config config) throws IOException {
        final SubjectMappingStreamRDF mapper = new SubjectMappingStreamRDF(config.getSource(),
                                                                           config.getDestination());
        try (final InputStream in2 = in) {
            RDFDataMgr.parse(mapper, in2, contentTypeToLang(config.getRdfLanguage()));
        }
        return mapper.getModel();
    }
}
