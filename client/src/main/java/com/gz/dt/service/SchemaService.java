package com.gz.dt.service;

import com.gz.dt.exception.ServiceException;
import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.IOUtils;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by naonao on 2015/10/28.
 */
public class SchemaService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "SchemaService.";

    public static final String WF_CONF_SCHEMAS = CONF_PREFIX + "wf.schemas";

    public static final String WF_CONF_EXT_SCHEMAS = CONF_PREFIX + "wf.ext.schemas";

    public static final String COORD_CONF_SCHEMAS = CONF_PREFIX + "coord.schemas";

    public static final String COORD_CONF_EXT_SCHEMAS = CONF_PREFIX + "coord.ext.schemas";

    public static final String BUNDLE_CONF_SCHEMAS = CONF_PREFIX + "bundle.schemas";

    public static final String BUNDLE_CONF_EXT_SCHEMAS = CONF_PREFIX + "bundle.ext.schemas";

    public static final String SLA_CONF_SCHEMAS = CONF_PREFIX + "sla.schemas";

    public static final String SLA_CONF_EXT_SCHEMAS = CONF_PREFIX + "sla.ext.schemas";

    @Deprecated
    public static final String SLA_NAME_SPACE_URI = "uri:oozie:sla:0.1";

    public static final String SLA_NAMESPACE_URI_2 = "uri:oozie:sla:0.2";

    public static final String COORDINATOR_NAMESPACE_URI_1 = "uri:oozie:coordinator:0.1";

    private Schema wfSchema;

    private Schema coordSchema;

    private Schema bundleSchema;

    private Schema slaSchema;


    private Schema loadSchema(String baseSchemas, String extSchema) throws SAXException, IOException {
        Set<String> schemaNames = new HashSet<String>();
        String[] schemas = ConfigurationService.getStrings(baseSchemas);
        if (schemas != null) {
            for (String schema : schemas) {
                schema = schema.trim();
                if (!schema.isEmpty()) {
                    schemaNames.add(schema);
                }
            }
        }
        schemas = ConfigurationService.getStrings(extSchema);
        if (schemas != null) {
            for (String schema : schemas) {
                schema = schema.trim();
                if (!schema.isEmpty()) {
                    schemaNames.add(schema);
                }
            }
        }
        List<StreamSource> sources = new ArrayList<StreamSource>();
        for (String schemaName : schemaNames) {
            sources.add(new StreamSource(IOUtils.getResourceAsStream(schemaName, -1)));
        }
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        return factory.newSchema(sources.toArray(new StreamSource[sources.size()]));
    }






    public void init(Services services) throws ServiceException {
        try {
            wfSchema = loadSchema(WF_CONF_SCHEMAS, WF_CONF_EXT_SCHEMAS);
            coordSchema = loadSchema(COORD_CONF_SCHEMAS, COORD_CONF_EXT_SCHEMAS);
            bundleSchema = loadSchema(BUNDLE_CONF_SCHEMAS, BUNDLE_CONF_EXT_SCHEMAS);
            slaSchema = loadSchema(SLA_CONF_SCHEMAS, SLA_CONF_EXT_SCHEMAS);
        }
        catch (SAXException ex) {
            throw new ServiceException(ErrorCode.E0130, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0131, ex.getMessage(), ex);
        }

    }

    public void destroy() {
        wfSchema = null;
        bundleSchema = null;
        slaSchema = null;
        coordSchema = null;

    }

    public Class<? extends Service> getInterface() {
        return SchemaService.class;
    }


    public Schema getSchema(SchemaName schemaName) {
        Schema returnSchema = null;
        if (schemaName == SchemaName.WORKFLOW) {
            returnSchema = wfSchema;
        }
        else if (schemaName == SchemaName.COORDINATOR) {
            returnSchema = coordSchema;
        }
        else if (schemaName == SchemaName.BUNDLE) {
            returnSchema = bundleSchema;
        }
        else if (schemaName == SchemaName.SLA_ORIGINAL) {
            returnSchema = slaSchema;
        }
        else {
            throw new RuntimeException("No schema found with name " + schemaName);
        }
        return returnSchema;
    }


    public enum SchemaName {
        WORKFLOW(1), COORDINATOR(2), SLA_ORIGINAL(3), BUNDLE(4);
        private final int id;

        private SchemaName(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

}
