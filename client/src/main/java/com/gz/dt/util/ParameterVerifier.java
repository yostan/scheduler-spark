package com.gz.dt.util;

import com.gz.dt.exception.ParameterVerifierException;
import org.apache.hadoop.conf.Configuration;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by naonao on 2015/10/28.
 */
public class ParameterVerifier {

    private static final Pattern nsVersionPattern = Pattern.compile("uri:oozie:(workflow|coordinator|bundle):(\\d+.\\d+)");

    private static final double workflowMinVersion = 0.4;
    private static final double coordinatorMinVersion = 0.4;
    private static final double bundleMinVersion = 0.2;


    public static void verifyParameters(Configuration conf, Element rootElement) throws ParameterVerifierException {
        ParamChecker.notNull(conf, "conf");
        if (rootElement == null) {
            return;
        }

        if (supportsParameters(rootElement.getNamespaceURI())) {
            Element params = rootElement.getChild("parameters", rootElement.getNamespace());
            if (params != null) {
                int numMissing = 0;
                StringBuilder missingParameters = new StringBuilder();
                Namespace paramsNs = params.getNamespace();
                Iterator<Element> it = params.getChildren("property", paramsNs).iterator();
                while (it.hasNext()) {
                    Element prop = it.next();
                    String name = prop.getChildTextTrim("name", paramsNs);
                    if (name != null) {
                        if (name.isEmpty()) {
                            throw new ParameterVerifierException(ErrorCode.E0739);
                        }
                        if (conf.get(name) == null) {
                            String defaultValue = prop.getChildTextTrim("value", paramsNs);
                            if (defaultValue != null) {
                                conf.set(name, defaultValue);
                            } else {
                                missingParameters.append(name);
                                missingParameters.append(", ");
                                numMissing++;
                            }
                        }
                    }
                }
                if (numMissing > 0) {
                    missingParameters.setLength(missingParameters.length() - 2);    //remove the trailing ", "
                    throw new ParameterVerifierException(ErrorCode.E0738, numMissing, missingParameters.toString());
                }
            } else {
                // Log a warning when the <parameters> section is missing
                XLog.getLog(ParameterVerifier.class).warn("The application does not define formal parameters in its XML "
                        + "definition");
            }
        }
    }


    static boolean supportsParameters(String namespaceURI) {
        boolean supports = false;
        Matcher m = nsVersionPattern.matcher(namespaceURI);
        if (m.matches() && m.groupCount() == 2) {
            String type = m.group(1);
            double ver = Double.parseDouble(m.group(2));
            supports = ((type.equals("workflow") && ver >= workflowMinVersion) ||
                    (type.equals("coordinator") && ver >= coordinatorMinVersion) ||
                    (type.equals("bundle") && ver >= bundleMinVersion));
        }
        return supports;
    }





}
