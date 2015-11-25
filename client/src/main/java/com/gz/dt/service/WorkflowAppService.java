package com.gz.dt.service;

import com.gz.dt.exception.HadoopAccessorException;
import com.gz.dt.exception.WorkflowException;
import com.gz.dt.util.ErrorCode;
import com.gz.dt.util.IOUtils;
import com.gz.dt.workflow.WorkflowApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by naonao on 2015/10/27.
 */
public abstract class WorkflowAppService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "WorkflowAppService.";
    public static final String SYSTEM_LIB_PATH = CONF_PREFIX + "system.libpath";
    public static final String APP_LIB_PATH_LIST = "oozie.wf.application.lib";
    public static final String HADOOP_USER = "user.name";
    public static final String CONFG_MAX_WF_LENGTH = CONF_PREFIX + "WorkflowDefinitionMaxLength";
    public static final String OOZIE_SUBWORKFLOW_CLASSPATH_INHERITANCE = "oozie.subworkflow.classpath.inheritance";
    public static final String OOZIE_WF_SUBWORKFLOW_CLASSPATH_INHERITANCE = "oozie.wf.subworkflow.classpath.inheritance";

    private Path systemLibPath;
    private long maxWFLength;
    private boolean oozieSubWfCPInheritance;

    public void init(Services services) {
        Configuration conf = services.getConf();

        String path = ConfigurationService.get(conf, SYSTEM_LIB_PATH);
        if (path.trim().length() > 0) {
            systemLibPath = new Path(path.trim());
        }

        maxWFLength = conf.getInt(CONFG_MAX_WF_LENGTH, 100000);

        oozieSubWfCPInheritance = conf.getBoolean(OOZIE_SUBWORKFLOW_CLASSPATH_INHERITANCE, false);
    }

    public void destroy() {
    }

    public Class<? extends Service> getInterface() {
        return WorkflowAppService.class;
    }



    protected String readDefinition(String appPath, String user, Configuration conf)
            throws WorkflowException {
        try {
            URI uri = new URI(appPath);
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration jobConf = has.createJobConf(uri.getAuthority());

            Configuration simpleConf = new Configuration();
            simpleConf.set("fs.defaultFS", jobConf.get("fs.defaultFS"));

            FileSystem fs = has.createFileSystem(user, uri, simpleConf);

            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                path = new Path(path, "workflow.xml");
            }

            FileStatus fsStatus = fs.getFileStatus(path);
            if (fsStatus.getLen() > this.maxWFLength) {
                throw new WorkflowException(ErrorCode.E0736, fsStatus.getLen(), this.maxWFLength);
            }

            Reader reader = new InputStreamReader(fs.open(path));
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();

        }
        catch (WorkflowException wfe) {
            throw wfe;
        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            throw new WorkflowException(ErrorCode.E0711, appPath, ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new WorkflowException(ex);
        }
        catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
    }



    public abstract WorkflowApp parseDef(Configuration jobConf) throws WorkflowException;

    public abstract WorkflowApp parseDef(Configuration jobConf, Configuration configDefault) throws WorkflowException;

    public abstract WorkflowApp parseDef(String wfXml, Configuration jobConf) throws WorkflowException;





}
