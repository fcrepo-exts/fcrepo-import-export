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
    public static Model mapRdfStreamToNonversionedSubjects(final InputStream in, final Config config) throws IOException {
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
