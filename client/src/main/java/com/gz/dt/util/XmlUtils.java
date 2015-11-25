package com.gz.dt.util;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Created by naonao on 2015/10/28.
 */
public class XmlUtils {

    public static Element parseXml(String xmlStr) throws JDOMException {
        ParamChecker.notNull(xmlStr, "xmlStr");
        try {
            SAXBuilder saxBuilder = createSAXBuilder();
            Document document = saxBuilder.build(new StringReader(xmlStr));
            return document.getRootElement();
        }
        catch (IOException ex) {
            throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
        }
    }


    private static SAXBuilder createSAXBuilder() {
        SAXBuilder saxBuilder = new SAXBuilder();

        //THIS IS NOT WORKING
        //saxBuilder.setFeature("http://xml.org/sax/features/external-general-entities", false);

        //INSTEAD WE ARE JUST SETTING AN EntityResolver that does not resolve entities
        saxBuilder.setEntityResolver(new NoExternalEntityEntityResolver());
        return saxBuilder;
    }



    private static class NoExternalEntityEntityResolver implements EntityResolver {

        public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
            return new InputSource(new ByteArrayInputStream(new byte[0]));
        }

    }


    public static PrettyPrint prettyPrint(Element element) {
        return new PrettyPrint(element);

    }


    public static class PrettyPrint {
        private String str;
        private Element element;

        private PrettyPrint(String str) {
            this.str = str;
        }

        private PrettyPrint(Element element) {
            this.element = ParamChecker.notNull(element, "element");
        }

        /**
         * Return the pretty print representation of an XML document.
         *
         * @return the pretty print representation of an XML document.
         */
        @Override
        public String toString() {
            if (str != null) {
                return str;
            }
            else {
                XMLOutputter outputter = new XMLOutputter();
                StringWriter stringWriter = new StringWriter();
                outputter.setFormat(Format.getPrettyFormat());
                try {
                    outputter.output(element, stringWriter);
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return stringWriter.toString();
            }
        }
    }



}
