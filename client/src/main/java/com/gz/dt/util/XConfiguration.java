package com.gz.dt.util;

import com.gz.dt.service.ConfigurationService;
import com.gz.dt.service.Services;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by naonao on 2015/10/26.
 */
public class XConfiguration extends Configuration {
    public static final String CONFIGURATION_SUBSTITUTE_DEPTH = "oozie.configuration.substitute.depth";

    public XConfiguration() {
        super(false);
        initSubstituteDepth();
    }

    public XConfiguration(InputStream is) throws IOException {
        this();
        parse(is);
    }

    public XConfiguration(Reader reader) throws IOException {
        this();
        parse(reader);
    }

    public XConfiguration(Properties props) {
        this();
        for (Map.Entry entry : props.entrySet()) {
            set((String) entry.getKey(), (String) entry.getValue());
        }

    }

    public Properties toProperties() {
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : this) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        return props;
    }


    @Override
    public String get(String name) {
        return substituteVars(getRaw(name));
    }

    @Override
    public String get(String name, String defaultValue) {
        String value = getRaw(name);
        if (value == null) {
            value = defaultValue;
        }
        else {
            value = substituteVars(value);
        }
        return value;
    }


    private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
    private static int MAX_SUBST = 20;
    protected static volatile boolean initalized = false;
    private static void initSubstituteDepth() {
        if (!initalized && Services.get() != null && Services.get().get(ConfigurationService.class) != null) {
            MAX_SUBST = ConfigurationService.getInt(CONFIGURATION_SUBSTITUTE_DEPTH);
            initalized = true;
        }
    }


    private String substituteVars(String expr) {
        if (expr == null) {
            return null;
        }
        Matcher match = varPat.matcher("");
        String eval = expr;
        int s = 0;
        while (MAX_SUBST == -1 || s < MAX_SUBST ) {
            match.reset(eval);
            if (!match.find()) {
                return eval;
            }
            String var = match.group();
            var = var.substring(2, var.length() - 1); // remove ${ .. }

            String val = getRaw(var);
            if (val == null) {
                val = System.getProperty(var);
            }

            if (val == null) {
                return eval; // return literal ${var}: var is unbound
            }
            // substitute
            eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
            s++;
        }
        throw new IllegalStateException("Variable substitution depth too large: " + MAX_SUBST + " " + expr);
    }


    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        return super.getClassByName(name.trim());
    }


    public static void copy(Configuration source, Configuration target) {
        for (Map.Entry<String, String> entry : source) {
            target.set(entry.getKey(), entry.getValue());
        }
    }


    public static void injectDefaults(Configuration source, Configuration target) {
        if (source != null) {
            for (Map.Entry<String, String> entry : source) {
                if (target.get(entry.getKey()) == null) {
                    target.set(entry.getKey(), entry.getValue());
                }
            }
        }
    }


    public XConfiguration trim() {
        XConfiguration trimmed = new XConfiguration();
        for (Map.Entry<String, String> entry : this) {
            trimmed.set(entry.getKey(), entry.getValue().trim());
        }
        return trimmed;
    }


    public XConfiguration resolve() {
        XConfiguration resolved = new XConfiguration();
        for (Map.Entry<String, String> entry : this) {
            resolved.set(entry.getKey(), get(entry.getKey()));
        }
        return resolved;
    }


    private void parse(InputStream is) throws IOException {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            // support for includes in the xml file
            docBuilderFactory.setNamespaceAware(true);
            docBuilderFactory.setXIncludeAware(true);
            // ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(is);
            parseDocument(doc);

        }
        catch (SAXException e) {
            throw new IOException(e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }


    private void parse(Reader reader) throws IOException {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            // support for includes in the xml file
            docBuilderFactory.setNamespaceAware(true);
            docBuilderFactory.setXIncludeAware(true);
            // ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(reader));
            parseDocument(doc);
        }
        catch (SAXException e) {
            throw new IOException(e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }


    private void parseDocument(Document doc) throws IOException {
        Element root = doc.getDocumentElement();
        if (!"configuration".equals(root.getTagName())) {
            throw new IOException("bad conf file: top-level element not <configuration>");
        }
        processNodes(root);
    }


    private void processNodes(Element root) throws IOException {
        try {
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if (prop.getTagName().equals("configuration")) {
                    processNodes(prop);
                    continue;
                }
                if (!"property".equals(prop.getTagName())) {
                    throw new IOException("bad conf file: element not <property>");
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
                        attr = ((Text) field.getFirstChild()).getData().trim();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
                        value = ((Text) field.getFirstChild()).getData();
                    }
                }
                if (attr != null && value != null) {
                    set(attr, value);
                }
            }

        }
        catch (DOMException e) {
            throw new IOException(e);
        }
    }


    public String toXmlString() {
        return toXmlString(true);
    }


    public String toXmlString(boolean prolog) {
        String xml;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            this.writeXml(baos);
            baos.close();
            xml = new String(baos.toByteArray());
        }
        catch (IOException ex) {
            throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
        }
        if (!prolog) {
            xml = xml.substring(xml.indexOf("<configuration>"));
        }
        return xml;
    }


    public String[] getTrimmedStrings(String name) {
        String[] values = getStrings(name);
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                values[i] = values[i].trim();
            }
        }
        return values;
    }


}
