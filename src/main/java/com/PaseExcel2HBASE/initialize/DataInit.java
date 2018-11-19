package com.PaseExcel2HBASE.initialize;

import com.alibaba.fastjson.JSONArray;
import com.PaseExcel2HBASE.Dao.HbaseDao;
import com.PaseExcel2HBASE.Service.PaseExcelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * 初始化操作
 */

public class DataInit {

    private static final Logger logger = LoggerFactory.getLogger(DataInit.class);

    public static String HBASE_ZOOKEEPER_QUORUM;
    public static Integer HBASE_ZOOKEEPER_TIMEOUT;
    public static String HBASE_PORT;
    public static String HBASE_MASTER;
    public static String HADOOP_HOME_DIR;
    public static String HBASE_TABLE;
    public static String HBASE_FAMILYNAME;
    public static String HBASE_TYPENAME;

    //文件夹路径
    public static String root_path;
    //文件属性（话单，账单，基站）
    public static String excelType;
    //excel文件集合
    public static List<File> fileList;
    //具体某一个excel文件
    public static File excelFile;
    //解析excel文件返回的json数组
    public static JSONArray jsonArray;
    //解析excel入HBASE
    public static PaseExcelService paseExcelService;
    public static HbaseDao dao;

    public void loadProperty() throws IOException {

        logger.info("开始加载模块properties");
        Properties properties = new Properties();

        InputStream in = DataInit.class.getClassLoader().getResourceAsStream("base.properties");
        properties.load(in);
        DataInit.root_path=properties.getProperty("root_path");

        logger.info("加载HBase配置文件");

        DataInit.HBASE_TABLE = new String(properties.getProperty("HBASE_TABLE").getBytes("ISO8859-1"), "utf-8");
        logger.info(String.format("HBASE_TABLE:[%s]", DataInit.HBASE_TABLE));

        DataInit.HBASE_FAMILYNAME = new String(properties.getProperty("HBASE_FAMILYNAME").getBytes("ISO8859-1"), "utf-8");
        logger.info(String.format("HBASE_FAMILYNAME:[%s]", DataInit.HBASE_FAMILYNAME));

        DataInit.root_path = new String(properties.getProperty("root_path"));
        logger.info(String.format("root_path:[%s]", DataInit.root_path));

        logger.info("加载HBase配置文件完成！");

    }

}
