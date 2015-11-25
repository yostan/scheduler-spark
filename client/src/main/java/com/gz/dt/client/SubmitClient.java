package com.gz.dt.client;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Created by naonao on 2015/10/22.
 */
public class SubmitClient {

    public static final String OOZIE_HOME = "/home/bigdata/wjr";
    public static final String APP_PATH = "oozie.wf.application.path";
    public static final String COORDINATOR_APP_PATH = "oozie.coord.application.path";
    public static final String BUNDLE_APP_PATH = "oozie.bundle.application.path";

    private String baseUrl;
    public static final String USER_NAME = "user.name";
    private int retryCount = 4;
    public int debugMode = 0;

    private String protocolUrl;
    private boolean validatedVersion = false;
    private JSONArray supportedVersions;

    public static final long WS_PROTOCOL_VERSION_0 = 0;
    public static final long WS_PROTOCOL_VERSION_1 = 1;
    public static final long WS_PROTOCOL_VERSION = 2; // pointer to current version

    private final Map<String, String> headers = new HashMap<String, String>();

    protected SubmitClient() {}

    public SubmitClient(String baseUrl) {
        this.baseUrl = notEmpty(baseUrl, "baseUrl");
        if(!this.baseUrl.endsWith("/")) {
            this.baseUrl += "/";
        }
    }

    private static final ThreadLocal<String> USER_NAME_TL = new ThreadLocal<String>();


    public Properties createConfiguration(){
        Properties pros = new Properties();
        String userName = USER_NAME_TL.get();
        if(userName == null) {
            //System.setProperty("user.name", "bigdata");
            userName = System.getProperty("user.name");
        }
        pros.setProperty(USER_NAME, userName);
        return pros;
    }


    public String run(Properties conf) throws Exception {
        return (new JobSubmit(conf, true)).call();
    }


    public static String notEmpty(String str, String name) {
        if(str == null) {
            throw new IllegalArgumentException(name + "cannot be null");
        }
        if(str.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }
        return str;
    }

    public static <T> T notNull(T obj, String name) {
        if (obj == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        return obj;
    }

    public int getDebugMode() {
        return debugMode;
    }

    public synchronized void validateWSVersion() throws Exception {

        if(!validatedVersion) {
            supportedVersions = getSupportedProtocolVersions();
            if(supportedVersions == null) {
                throw new Exception("supportedVersions is null");
            }

            if (supportedVersions.contains(WS_PROTOCOL_VERSION)) {
                protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION + "/";
            }
            else if (supportedVersions.contains(WS_PROTOCOL_VERSION_1)) {
                protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION_1 + "/";
            }
            else {
                if (supportedVersions.contains(WS_PROTOCOL_VERSION_0)) {
                    protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION_0 + "/";
                }
            }
        }
        validatedVersion = true;
    }


    public JSONArray getSupportedProtocolVersions() throws Exception {
        JSONArray versions = null;
        final URL url = new URL(baseUrl + RestConstants.VERSIONS);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        if(conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
            versions = (JSONArray) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
        }
        else {
            throw new Exception("getSupportedProtocolVersions fail");
        }
        return versions;
    }


    public String getBaseURLForVersion(long protocolVersion) throws Exception {
        supportedVersions = getSupportedProtocolVersions();
        if(supportedVersions == null) {
            throw new Exception("supportedVersions is null");
        }
        if (supportedVersions.contains(protocolVersion)) {
            return baseUrl + "v" + protocolVersion + "/";
        }
        else {
            throw new Exception("protocolVersion is invaild");
        }
    }


    public URL createURL(Long protocolVersion, String collection, String resource, Map<String, String> parameters)
            throws Exception {
        validateWSVersion();
        StringBuilder sb = new StringBuilder();
        if(protocolVersion == null) {
            sb.append(protocolUrl);
        }
        else {
            sb.append(getBaseURLForVersion(protocolVersion));
        }
        sb.append(collection);
        if (resource != null && resource.length() > 0) {
            sb.append("/").append(resource);
        }
        if (parameters.size() > 0) {
            String separator = "?";
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                if (param.getValue() != null) {
                    sb.append(separator).append(URLEncoder.encode(param.getKey(), "UTF-8")).append("=").append(
                            URLEncoder.encode(param.getValue(), "UTF-8"));
                    separator = "&";
                }
            }
        }
        return new URL(sb.toString());
    }


    public int getRetryCount() {
        return retryCount;
    }


    protected HttpURLConnection createRetryableConnection(final URL url, final String method) throws Exception{
        int numTries = 0;
        boolean stopRetry = false;
        Exception cliException = null;

        while (numTries < retryCount && !stopRetry) {
            try {
                HttpURLConnection conn = createConnection(url, method);
                return conn;
            }
            catch (ConnectException e) {
                sleep(e, numTries++);
                cliException = e;
            }
            catch (SocketException e) {
                if (method.equals("POST") || method.equals("PUT")) {
                    stopRetry = true;
                }
                else {
                    sleep(e, numTries++);
                }
                cliException = e;
            }
            catch (Exception e) {
                stopRetry = true;
                cliException = e;
                numTries++;
                // No retry for other exceptions
            }
        }

        throw new Exception("Error while connecting Oozie server. No of retries = " + numTries + ". Exception = "
                + cliException.getMessage(), cliException);
    }


    protected HttpURLConnection createConnection(URL url, String method) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        if (method.equals("POST") || method.equals("PUT")) {
            conn.setDoOutput(true);
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            conn.setRequestProperty(header.getKey(), header.getValue());
        }
        return conn;
    }


    private void sleep(Exception e, int numTries) {
        try {
            long wait = ((long) Math.pow(2, numTries) * 1000L);
            System.err.println("Connection exception has occurred [ " + e.getClass().getName() + " " + e.getMessage()
                    + " ]. Trying after " + wait / 1000 + " sec." + " Retry count = " + (numTries + 1));
            Thread.sleep(wait);
        }
        catch (InterruptedException e1) {
            // Ignore InterruptedException
        }
    }

    static Map<String, String> prepareParams(String... params) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < params.length; i = i + 2) {
            map.put(params[i], params[i + 1]);
        }
        String doAsUserName = USER_NAME_TL.get();
        if (doAsUserName != null) {
            map.put(RestConstants.DO_AS_PARAM, doAsUserName);
        }
        return map;
    }

    public void writeToXml(Properties props, OutputStream out) throws IOException {
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element conf = doc.createElement("configuration");
            doc.appendChild(conf);
            conf.appendChild(doc.createTextNode("\n"));
            for (String name : props.stringPropertyNames()) { // Properties whose key or value is not of type String are omitted.
                String value = props.getProperty(name);
                Element propNode = doc.createElement("property");
                conf.appendChild(propNode);

                Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(name.trim()));
                propNode.appendChild(nameNode);

                Element valueNode = doc.createElement("value");
                valueNode.appendChild(doc.createTextNode(value.trim()));
                propNode.appendChild(valueNode);

                conf.appendChild(doc.createTextNode("\n"));
            }

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(out);
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            transformer.transform(source, result);
            if (getDebugMode() > 0) {
                result = new StreamResult(System.out);
                transformer.transform(source, result);
                System.out.println();
            }
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }


    protected abstract class ClientCallable<T> implements Callable<T> {
        private final String method;
        private final String collection;
        private final String resource;
        private final Map<String, String> params;
        private final Long protocolVersion;

        public ClientCallable(String method, String collection, String resource, Map<String, String> params) {
            this(method, null, collection, resource, params);
        }

        public ClientCallable(String method, Long protocolVersion, String collection, String resource, Map<String, String> params) {
            this.method = method;
            this.protocolVersion = protocolVersion;
            this.collection = collection;
            this.resource = resource;
            this.params = params;
        }

        public T call() throws Exception {
            URL url = createURL(protocolVersion, collection, resource, params);
            return call(createRetryableConnection(url, method));
        }

        protected abstract T call(HttpURLConnection conn) throws Exception;
    }


    private class JobSubmit extends ClientCallable<String> {
        private final Properties conf;

        JobSubmit(Properties conf, boolean start) {
            super("POST", RestConstants.JOBS, "", (start) ? prepareParams(RestConstants.ACTION_PARAM,
                    RestConstants.JOB_ACTION_START) : prepareParams());
            this.conf = notNull(conf, "conf");
        }

        JobSubmit(String jobId, Properties conf) {
            super("PUT", RestConstants.JOB, notEmpty(jobId, "jobId"), prepareParams(RestConstants.ACTION_PARAM,
                    RestConstants.JOB_ACTION_RERUN));
            this.conf = notNull(conf, "conf");
        }

        public JobSubmit(Properties conf, String jobActionDryrun) {
            super("POST", RestConstants.JOBS, "", prepareParams(RestConstants.ACTION_PARAM,
                    RestConstants.JOB_ACTION_DRYRUN));
            this.conf = notNull(conf, "conf");
        }

        @Override
        protected String call(HttpURLConnection conn) throws Exception {
            conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
            writeToXml(conf, conn.getOutputStream());
            int code = conn.getResponseCode();
            if (conn.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
//                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
//                return (String) json.get(JsonTags.JOB_ID);
                return  ((JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()))).toJSONString();
                //return conn.getURL().toString();
            }

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new Exception("http error code: "+conn.getResponseCode());
            }
            return null;
        }
    }


    public static enum SYSTEM_MODE {
        NORMAL, NOWEBSERVICE, SAFEMODE
    }

}
