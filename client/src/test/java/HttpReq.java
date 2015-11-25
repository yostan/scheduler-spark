import com.gz.dt.client.SubmitClient;

import java.util.Properties;

/**
 * Created by naonao on 2015/10/26.
 */

public class HttpReq {

    public static void main(String[] args) throws Exception {
        SubmitClient client = new SubmitClient("http://master:8099/webapp/");
        Properties pros = client.createConfiguration();
        pros.setProperty(SubmitClient.APP_PATH, "hdfs://master:9000/bigdata/wjr");
        //pros.setProperty("oozie.home.dir","E:\\shmai\\scrapygithub\\prj\\scheduler-spark\\client\\src\\main");
        //pros.setProperty(SubmitClient.APP_PATH, "hdfs://foo:8020/usr/tucu/my-wf-app");
//        pros.setProperty("jobTracker", "foo:8021");
//        pros.setProperty("inputDir", "/usr/tucu/inputdir");
//        pros.setProperty("outputDir", "/usr/tucu/outputdir");
        String url = client.run(pros);
        System.out.println("url: "+url);
    }
}
