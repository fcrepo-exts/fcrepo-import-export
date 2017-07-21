package org.fcrepo.importexport.importer;

import static org.fcrepo.importexport.common.FcrepoConstants.BINARY_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.EXTERNAL_RESOURCE_EXTENSION;
import static org.fcrepo.importexport.common.FcrepoConstants.FCR_METADATA_PATH;
import static org.fcrepo.importexport.common.FcrepoConstants.RDF_TYPE;
import static org.fcrepo.importexport.common.ModelUtils.mapRdfStreamToNonversionedSubjects;
import static org.fcrepo.importexport.common.URITranslationUtil.addRelativePath;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.fcrepo.importexport.common.Config;
import org.fcrepo.importexport.common.TransferProcess;


/**
 * An event for importing a resource
 * 
 * @author bbpennel
 *
 */
public class ImportResource extends ImportEvent {

    private URI descriptionUri;
    private Model model;
    private Resource resource;
    private boolean isVersion;
    private File binary;
    private File descriptionFile;

    /**
     * Construct new ImportResource
     * 
     * @param id id
     * @param uri uri of resource
     * @param descriptionFile description file for resource
     * @param timestamp last modified timestamp for resource
     * @param config config
     */
    public ImportResource(final String id, final URI uri, final File descriptionFile, final long timestamp,
            final Config config) {
        super(id, uri, timestamp, config);
        this.descriptionFile = descriptionFile;
    }

    /**
     * Get the URI for metadata for this resource
     * 
     * @return uri of the RDF endpoint for resource
     */
    public URI getDescriptionUri() {
        if (descriptionUri == null) {
            if (isBinary()) {
                descriptionUri = addRelativePath(getMappedUri(), FCR_METADATA_PATH);
            } else {
                descriptionUri = getMappedUri();
            }
        }
        return descriptionUri;
    }

    /**
     * Test if this resource is a binary
     * 
     * @return true if resource is a binary
     */
    public boolean isBinary() {
        return getBinary().exists();
    }

    /**
     * Get the binary file for this resource
     * 
     * @return the binary for this resource or null if not found
     */
    public File getBinary() {
        if (binary == null) {
            binary = TransferProcess.fileForURI(uri, config.getSourcePath(),
                    config.getDestinationPath(), config.getBaseDirectory(), BINARY_EXTENSION);
            if (!binary.exists()) {
                binary = TransferProcess.fileForURI(uri, config.getSourcePath(),
                        config.getDestinationPath(), config.getBaseDirectory(), EXTERNAL_RESOURCE_EXTENSION);
            }
        }
        return binary;
    }
    
    /**
     * Get the file containing metadata for this resource
     * 
     * @return returns the file containing this resource's description
     */
    public File getDescriptionFile() {
        return descriptionFile;
    }

    /**
     * Get the model containing properties assigned to this resource
     * 
     * @return model containing properties assigned to this resource
     */
    public Model getModel() {
        if (model == null) {
            final File mdFile = getDescriptionFile();
            if (mdFile == null) {
                return null;
            }
            try {
                model = mapRdfStreamToNonversionedSubjects(new FileInputStream(mdFile), config);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read model for " + id, e);
            }
        }
        return model;
    }



    /**
     * Get the resource representing this ImportResource from its model
     * 
     * @return rdf resource for this resource
     */
    public Resource getResource() {
        if (resource == null) {
            final Model model = getModel();
            if (model == null) {
                return null;
            }
            resource = model.listResourcesWithProperty(RDF_TYPE).next();
        }
        return resource;
    }

    /**
     * Return true if this resource is a Version
     * 
     * @return true if this resource is a Version
     */
    public boolean isVersion() {
        return isVersion;
    }

    /**
     * Setter for isVersion property
     * 
     * @param isVersion value to set
     */
    public void setIsVersion(final boolean isVersion) {
        this.isVersion = isVersion;
    }
}
