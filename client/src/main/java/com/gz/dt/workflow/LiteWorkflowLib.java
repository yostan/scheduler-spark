package com.gz.dt.workflow;

import com.gz.dt.exception.WorkflowException;
import com.gz.dt.util.ParamChecker;
import org.apache.hadoop.conf.Configuration;

import javax.xml.validation.Schema;
import java.io.StringReader;

/**
 * Created by naonao on 2015/10/28.
 */
public abstract class LiteWorkflowLib implements WorkflowLib {

    private Schema schema;

    public LiteWorkflowLib(Schema schema/*,
                           Class<? extends ControlNodeHandler> controlNodeHandler,
                           Class<? extends DecisionNodeHandler> decisionHandlerClass,
                           Class<? extends ActionNodeHandler> actionHandlerClass*/) {
        this.schema = schema;
//        this.controlHandlerClass = controlNodeHandler;
//        this.decisionHandlerClass = decisionHandlerClass;
//        this.actionHandlerClass = actionHandlerClass;
    }


    public WorkflowApp parseDef(String appXml, Configuration jobConf, Configuration configDefault) throws WorkflowException {
        ParamChecker.notEmpty(appXml, "appXml");

        return new LiteWorkflowAppParser(schema)
                .validateAndParse(new StringReader(appXml), jobConf, configDefault);
    }
}
