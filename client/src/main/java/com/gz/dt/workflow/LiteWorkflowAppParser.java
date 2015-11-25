package com.gz.dt.workflow;

import com.gz.dt.exception.ParameterVerifierException;
import com.gz.dt.exception.WorkflowException;
import com.gz.dt.service.ConfigurationService;
import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.IOUtils;
import com.gz.dt.util.ParameterVerifier;
import com.gz.dt.util.XmlUtils;
import org.apache.hadoop.conf.Configuration;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by naonao on 2015/10/28.
 */
public class LiteWorkflowAppParser {

    private static final String DECISION_E = "decision";
    private static final String ACTION_E = "action";
    private static final String END_E = "end";
    private static final String START_E = "start";
    private static final String JOIN_E = "join";
    private static final String FORK_E = "fork";
    private static final Object KILL_E = "kill";

    private static final String SLA_INFO = "info";
    private static final String CREDENTIALS = "credentials";
    private static final String GLOBAL = "global";
    private static final String PARAMETERS = "parameters";

    private static final String NAME_A = "name";
    private static final String CRED_A = "cred";
    private static final String USER_RETRY_MAX_A = "retry-max";
    private static final String USER_RETRY_INTERVAL_A = "retry-interval";
    private static final String TO_A = "to";

    private static final String FORK_PATH_E = "path";
    private static final String FORK_START_A = "start";

    private static final String ACTION_OK_E = "ok";
    private static final String ACTION_ERROR_E = "error";

    private static final String DECISION_SWITCH_E = "switch";
    private static final String DECISION_CASE_E = "case";
    private static final String DECISION_DEFAULT_E = "default";

    private static final String KILL_MESSAGE_E = "message";
    public static final String VALIDATE_FORK_JOIN = "oozie.validate.ForkJoin";
    public static final String WF_VALIDATE_FORK_JOIN = "oozie.wf.validate.ForkJoin";

    public static final String DEFAULT_NAME_NODE = "oozie.actions.default.name-node";
    public static final String DEFAULT_JOB_TRACKER = "oozie.actions.default.job-tracker";

    private static final String JOB_TRACKER = "job-tracker";
    private static final String NAME_NODE = "name-node";
    private static final String JOB_XML = "job-xml";
    private static final String CONFIGURATION = "configuration";

    private Schema schema;
//    private Class<? extends ControlNodeHandler> controlNodeHandler;
//    private Class<? extends DecisionNodeHandler> decisionHandlerClass;
//    private Class<? extends ActionNodeHandler> actionHandlerClass;

    private List<String> forkList = new ArrayList<String>();
    private List<String> joinList = new ArrayList<String>();
//    private StartNodeDef startNode;
//    private List<NodeAndTopDecisionParent> visitedOkNodes = new ArrayList<NodeAndTopDecisionParent>();
    private List<String> visitedJoinNodes = new ArrayList<String>();

    private String defaultNameNode;
    private String defaultJobTracker;






    public LiteWorkflowAppParser(Schema schema/*,
                                 Class<? extends ControlNodeHandler> controlNodeHandler,
                                 Class<? extends DecisionNodeHandler> decisionHandlerClass,
                                 Class<? extends ActionNodeHandler> actionHandlerClass*/) throws WorkflowException {
        this.schema = schema;
//        this.controlNodeHandler = controlNodeHandler;
//        this.decisionHandlerClass = decisionHandlerClass;
//        this.actionHandlerClass = actionHandlerClass;

        defaultNameNode = ConfigurationService.get(DEFAULT_NAME_NODE);
        if (defaultNameNode != null) {
            defaultNameNode = defaultNameNode.trim();
            if (defaultNameNode.isEmpty()) {
                defaultNameNode = null;
            }
        }
        defaultJobTracker = ConfigurationService.get(DEFAULT_JOB_TRACKER);
        if (defaultJobTracker != null) {
            defaultJobTracker = defaultJobTracker.trim();
            if (defaultJobTracker.isEmpty()) {
                defaultJobTracker = null;
            }
        }
    }


    public LiteWorkflowApp validateAndParse(Reader reader, Configuration jobConf) throws WorkflowException {
        return validateAndParse(reader, jobConf, null);
    }


    public LiteWorkflowApp validateAndParse(Reader reader, Configuration jobConf, Configuration configDefault)
            throws WorkflowException {
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            String strDef = writer.toString();

            if (schema != null) {
                Validator validator = schema.newValidator();
                validator.validate(new StreamSource(new StringReader(strDef)));
            }

            Element wfDefElement = XmlUtils.parseXml(strDef);
            ParameterVerifier.verifyParameters(jobConf, wfDefElement);
            LiteWorkflowApp app = parse(strDef, wfDefElement, configDefault, jobConf);
//            Map<String, VisitStatus> traversed = new HashMap<String, VisitStatus>();
//            traversed.put(app.getNode(StartNodeDef.START).getName(), VisitStatus.VISITING);
//            validate(app, app.getNode(StartNodeDef.START), traversed);
//            //Validate whether fork/join are in pair or not
//            if (jobConf.getBoolean(WF_VALIDATE_FORK_JOIN, true)
//                    && ConfigurationService.getBoolean(VALIDATE_FORK_JOIN)) {
//                validateForkJoin(app);
//            }
            return app;
        }
        catch (ParameterVerifierException ex) {
            throw new WorkflowException(ex);
        }
        catch (JDOMException ex) {
            throw new WorkflowException(ErrorCode.E0700, ex.getMessage(), ex);
        }
        catch (SAXException ex) {
            throw new WorkflowException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }



    private LiteWorkflowApp parse(String strDef, Element root, Configuration configDefault, Configuration jobConf)
            throws WorkflowException {
        Namespace ns = root.getNamespace();
        LiteWorkflowApp def = null;
//        GlobalSectionData gData = null;

        for (Element eNode : (List<Element>) root.getChildren()) {
            if (eNode.getName().equals(START_E)) {
                def = new LiteWorkflowApp(root.getAttributeValue(NAME_A), strDef/*,
                        new StartNodeDef(controlNodeHandler, eNode.getAttributeValue(TO_A))*/);
            }
        }
        return def;
    }


}
