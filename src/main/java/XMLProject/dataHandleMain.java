package XMLProject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class dataHandleMain {

    static Configuration config = new Configuration();
    static final Logger logger = Logger.getLogger(dataHandleMain.class);
//     定义表的截取长度集合
    static Collection<String> tabeLengthList = new ArrayList<String>();
    static FSDataOutputStream out = null;
    static String tbFieldLength = "  ";

    public static void main(String[] args) {

//		System.load("C:/hadoop-2.5.0-cdh5.3.6/bin/hadoop.dll");

        SimpleDateFormat df = new SimpleDateFormat("HHmmss");
        System.setProperty("log_time", df.format(new Date()));
        System.setProperty("log_data", args[0]);
        System.setProperty("log_name", args[1]);

        /**
         * 初始化信息
         */
        initTableInfo.initTableName();
        initTableInfo.initProper();
//		checkFiles.getAllFileName(args[0]);

        dataHandleMain dhm = new dataHandleMain();

        /**
         * 初始化表名对应的长度字段的长度
         */

        initTableInfo.initTableCutLength(args[1]);
        String srcFile = args[0] + "/" + args[1] + "_" + args[0] + ".dat";
        String tmpFile = args[0] + "/" + args[1] + "_" + "tmp";
        config.set("fieldLength", initTableInfo.tableCutLength);
        // mapreduce 参数 ：input 与  output
        args = new String[]{
                initTableInfo.input + args[0] + "/" + args[1], initTableInfo.input + tmpFile
        };

        // run job
        try {
            if (!dhm.exists(args[1])) {
                logger.info("检查输出目录:" + args[1]);
            }

            int status = dhm.run(args);
            // exit program
            if (status != 0) {
                logger.error("mapReduce执行失败");
                System.exit(1);
            }
        } catch (ClassNotFoundException | IOException | InterruptedException | URISyntaxException e) {
            logger.error("map端运行出错" + e);
            System.exit(1);
        }

    }

    // 运行 run
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * get Configuration
         * create Job
         */
        Job job = Job.getInstance(config, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        /**
         * 设置job：set job
         * 输入文件路径：input
         * 设置map：map class
         * 设置reduce：reduce class
         * 输出文件路径：output
         * 提交job：submit job
         */
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);

        job.setMapperClass(dataHandleMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(dataHandleReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0 : 1;
    }

    /**
     * @param 需要判断的path
     * @return 判断路径是否存在
     * @throws URISyntaxException
     * @throws InterruptedException
     * @throws IOException
     */
    public boolean exists(String Path) throws IOException, InterruptedException, URISyntaxException {
        Path outPath = new Path(Path);
        FileSystem fs = FileSystem.get(new URI(initTableInfo.hadoopUrl), new Configuration(), initTableInfo.hadoopUser);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        return true;
    }

    /**
     * @param 需要判断的path
     * @return 是否删除成功
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    public boolean delete(String s) throws IOException, InterruptedException, URISyntaxException {
        Path p = new Path(s);
        FileSystem fs = FileSystem.get(new URI(initTableInfo.hadoopUrl), new Configuration(), initTableInfo.hadoopUser);
        return fs.delete(p, true);
    }
}
