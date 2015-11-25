package com.gz.dt.workflow;

import com.gz.dt.util.ParamChecker;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by naonao on 2015/10/28.
 */
public class LiteWorkflowApp implements Writable, WorkflowApp {

    private String name;
    private String definition;
//    private Map<String, NodeDef> nodesMap = new LinkedHashMap<String, NodeDef>();
    private boolean complete = false;

    LiteWorkflowApp() {
    }

    public LiteWorkflowApp(String name, String definition/*, StartNodeDef startNode*/) {
        this.name = ParamChecker.notEmpty(name, "name");
        this.definition = ParamChecker.notEmpty(definition, "definition");
//        nodesMap.put(StartNodeDef.START, startNode);
    }



    public String getName() {
        return name;
    }

    public String getDefinition() {
        return definition;
    }

    public void write(DataOutput dataOutput) throws IOException {

    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
