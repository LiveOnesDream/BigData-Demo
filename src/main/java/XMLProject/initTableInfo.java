package XMLProject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class initTableInfo {

    /**
     * 表名列表：tableNames
     * 获取表名的分割长度：tableCutLength
     */
    public static String[] tableNames;
    public static String tableCutLength;
    public static String input;
    public static String output;
    public static String hadoopUrl;
    public static String hadoopUser;
    public static String srcDataDir;

    private static Properties p = new Properties();

    static {
        InputStream is = initTableInfo.class.getClassLoader().getResourceAsStream("hadoop.properties");
        try {
            p.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 初始化表名数组
    public static String[] initTableName() {
        tableNames = p.getProperty("tableList").split(",");
        return tableNames;
    }

    // 初始化表剪切的长度
    public static String initTableCutLength(String tableName) {
        tableCutLength = p.getProperty(tableName);
        return tableCutLength;
    }

    /**
     * 获取输入文件路径： input
     * 获取输出文件路径： output
     * 获取Hadoop的url： hadoopUrl
     * 获取源数据目录：stcDataDir
     */
    public static void initProper() {
        input = p.getProperty("input");
        output = p.getProperty("output");
        hadoopUrl = p.getProperty("hadoopUrl");
        hadoopUser = p.getProperty("hadoopUser");
        srcDataDir = p.getProperty("srcDataDir");
    }

    // 获取输入文件路径： input
    public String getInput() {
        return input;
    }

    // 获取输出文件路径： output
    public String getOutput() {
        return output;
    }

    // 获取Hadoop的url： hadoopUrl
    public String getHadoopUrl() {
        return hadoopUrl;
    }

    // 获取Hadoop的user： hadoopuser
    public String gethadoopUser() {
        return hadoopUser;
    }

    // 获取Hadoop的user： hadoopuser
    public String getsrcDataDir() {
        return srcDataDir;
    }

}
