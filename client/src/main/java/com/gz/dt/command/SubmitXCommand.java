package com.gz.dt.command;

import com.gz.dt.client.SubmitClient;
import com.gz.dt.exception.CommandException;
import com.gz.dt.exception.HadoopAccessorException;
import com.gz.dt.exception.PreconditionException;
import com.gz.dt.exception.WorkflowException;
import com.gz.dt.service.HadoopAccessorService;
import com.gz.dt.service.Services;
import com.gz.dt.service.WorkflowAppService;
import com.gz.dt.util.*;
import com.gz.dt.workflow.WorkflowApp;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.deploy.SparkSubmit;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


/**
 * Created by naonao on 2015/10/27.
 */
@SuppressWarnings("deprecation")
public class SubmitXCommand extends XCommand<String> {

    public static final String CONFIG_DEFAULT = "config-default.xml";

    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();

    private Configuration conf;
    private List<JsonBean> insertList = new ArrayList<JsonBean>();
    private String parentId;

    public SubmitXCommand(Configuration conf) {
        super("submit", "submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
    }

    public SubmitXCommand(Configuration conf, String parentId) {
        this(conf);
        this.parentId = parentId;
    }

    public SubmitXCommand(boolean dryrun, Configuration conf) {
        this(conf);
        this.dryrun = dryrun;
    }

    static {
        String[] badUserProps = {PropertiesUtils.DAYS, PropertiesUtils.HOURS, PropertiesUtils.MINUTES,
                PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB, PropertiesUtils.TB, PropertiesUtils.PB,
                PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN, PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN,
                PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = {PropertiesUtils.HADOOP_USER};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }


    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    public java.lang.String getEntityKey() {
        return null;
    }

    @Override
    protected void loadState() throws CommandException {

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {

    }

    @Override
    protected String execute() throws CommandException {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);

        try {

            String user = conf.get(SubmitClient.USER_NAME);
            URI uri = new URI(conf.get(SubmitClient.APP_PATH));

            System.out.println("*********************************");
            System.out.println("APP_PATH: "+uri.getPath());
            System.out.println("*********************************");

            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createJobConf(uri.getAuthority());

            System.out.println("*********************************");
            System.out.println("NAMENODE: "+fsConf.get("fs.defaultFS"));
            System.out.println("*********************************");

            Configuration simpleConf = new Configuration();
            simpleConf.set("fs.defaultFS", fsConf.get("fs.defaultFS"));

            FileSystem fs = has.createFileSystem(user, uri, simpleConf);

            Path configDefault = null;
            Configuration defaultConf = null;
            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                configDefault = new Path(path, CONFIG_DEFAULT);
            } else {
                configDefault = new Path(path.getParent(), CONFIG_DEFAULT);
            }

            if (fs.exists(configDefault)) {
                try {
                    defaultConf = new XConfiguration(fs.open(configDefault));
                    PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                    XConfiguration.injectDefaults(defaultConf, conf);
                }
                catch (IOException ex) {
                    throw new IOException("default configuration file, " + ex.getMessage(), ex);
                }
            }

            WorkflowApp app = wps.parseDef(conf, defaultConf);
//            String actionxml = app.getDefinition();

            System.out.println("####################################");
            System.out.println("workflow.xml: \n"+app.getDefinition());
            System.out.println("####################################");


            Element eActionConf = null;
            Element rooot = XmlUtils.parseXml(app.getDefinition());
            for (Element eNode : (List<Element>) rooot.getChildren()) {
                if (eNode.getName().equals("action")) {

                    for (Element elem : (List<Element>) eNode.getChildren()) {
                        if("spark".equals(elem.getName())){
                            eActionConf = elem;
                        }
                    }
                }
            }


            submitSpark(eActionConf, fs);

        }catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
        catch (HadoopAccessorException ex) {
            throw new CommandException(ex);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0803, ex.getMessage(), ex);
        }

        return "OK";
    }


    public void submitSpark(Element eActionConf, FileSystem fs) {

        Namespace ns = eActionConf.getNamespace();

        Element prepareElement = eActionConf.getChild("prepare", ns);

        String master = eActionConf.getChildTextTrim("master", ns);
        String mode = eActionConf.getChildTextTrim("mode", ns);
        String name = eActionConf.getChildTextTrim("name", ns);
        String cls = eActionConf.getChildTextTrim("class", ns);
        String jar = eActionConf.getChildTextTrim("jar", ns);
        String sparkOpts = eActionConf.getChildTextTrim("spark-opts", ns);

        final String MASTER_OPTION = "--master";
        final String MODE_OPTION = "--deploy-mode";
        final String JOB_NAME_OPTION = "--name";
        final String CLASS_NAME_OPTION = "--class";
        final String VERBOSE_OPTION = "--verbose";
        final String DELIM = " ";

        try {

//            String cmd111 = "echo $HADOOP_CONF_DIR";
//            BufferedReader bf = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(cmd111).getInputStream()));
//            StringBuilder sb = new StringBuilder();
//            String line = null;
//            while ((line = bf.readLine()) != null) {
//                sb.append(line);
//            }
//            System.out.println("DEFAULT HADOOP_CONF_DIR: "+sb.toString());

            //export HADOOP_CONF_DIR, spark on yarn must be set
            Map<String, String> sysenv = System.getenv();

            System.out.println("--------------------------------------------");
            for(Map.Entry sys : sysenv.entrySet()) {
                System.out.println(sys.getKey()+"="+sys.getValue());

            }
            System.out.println("--------------------------------------------");


            //String[] cmd = {"/bin/sh", "-c", "export HADOOP_CONF_DIR=/home/bigdata/app/hadoop-2.6.1/etc/hadoop"}; //"export HADOOP_CONF_DIR=/home/bigdata/app/hadoop-2.6.1/etc/hadoop";

//            System.out.println("--------------------------------------------");
//            System.out.println("cmd: "+cmd);
//            System.out.println("--------------------------------------------");

//            Process process = Runtime.getRuntime().exec(cmd);
//            System.out.println("--------------------------------------------");
//            System.out.println("hadoop_conf: "+System.getenv("hadoop_conf_dir"));
//            System.out.println("--------------------------------------------");


            String prepareXML = "";
            if (prepareElement != null) {
                Element prepareDeleteNode = prepareElement.getChild("delete", ns);
                prepareXML = prepareDeleteNode.getAttributeValue("path");
                System.out.println("prepareXML: "+prepareXML);

                if(prepareXML != null && !prepareXML.equals("")) {
                    URI uri = new URI(prepareXML);
                    Path path = new Path(uri.getPath());
                    if (fs.exists(path)) {
                        fs.delete(path, true);
                        System.out.println("*********** deleted hdfs path: " + path +"***********");
                    }
                }
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        List<String> sparkArgs = new ArrayList<String>();

        sparkArgs.add(MASTER_OPTION);
        sparkArgs.add(master);

        if(mode != null && !mode.equals("")) {
            sparkArgs.add(MODE_OPTION);
            sparkArgs.add(mode);
        }

        if(name != null && !name.equals("")) {
            sparkArgs.add(JOB_NAME_OPTION);
            sparkArgs.add(name);
        }

        sparkArgs.add(CLASS_NAME_OPTION);
        sparkArgs.add(cls);

        if (StringUtils.isNotEmpty(sparkOpts)) {
            String[] sparkOptions = sparkOpts.split(DELIM);
            for (String opt : sparkOptions) {
                sparkArgs.add(opt);
            }
        }

        sparkArgs.add(jar);

        List<Element> list = eActionConf.getChildren("arg", ns);
        String[] argsss = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            argsss[i] = list.get(i).getTextTrim();
            sparkArgs.add(argsss[i]);
        }

        System.out.println("Spark Action Main class        : " + SparkSubmit.class.getName());
        System.out.println();
        System.out.println("Spark action configuration ");
        System.out.println("=================================================================");
        System.out.println();
        for (String arg : sparkArgs) {
            System.out.println("                    " + arg);
        }
        System.out.println();

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Spark class now >>>");
        System.out.println();
        System.out.flush();

        SparkSubmit.main(sparkArgs.toArray(new String[sparkArgs.size()]));
}

}
