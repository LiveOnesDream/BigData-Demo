package com.PaseExcel2HBASE.Service;

import com.PaseExcel2HBASE.initialize.DataInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

public class HandleFile {

    static final Logger logger = LoggerFactory.getLogger(HandleFile.class);

    /**
     * 获取文件夹下所有文件
     *
     * @return list<File>
     */
    public ArrayList<File> getFiles() {

        logger.info("加载 base.properties 基础配置文件");
        loadBaseProperty();
        ArrayList<File> files = new ArrayList<>();

        File file = new File("C:\\Users\\Administrator\\Desktop\\科瑞\\话单2");
        File[] tempList = file.listFiles();

        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                logger.info("文件：" + tempList[i]);
                files.add(new File(tempList[i].toString()));
            }
            if (tempList[i].isDirectory()) {
                logger.info("文件夹：" + tempList[i]);
            }
        }
        return files;
    }

    /**
     * 加载HBASE 连接配置文件
     */
    public void loadBaseProperty() {

        Properties properties = new Properties();
        InputStream in = HandleFile.class.getClassLoader().getResourceAsStream("base.properties");

        try {
            properties.load(in);
            String rootPath = new String(properties.getProperty("root_path").getBytes("ISO8859-1"), "utf-8");
            DataInit.root_path = rootPath;

            logger.info("加载Hbase基础配置信息");
            DataInit.HBASE_ZOOKEEPER_QUORUM = new String(properties.getProperty("HBASE_ZOOKEEPER_QUORUM").getBytes(), "utf-8");
            logger.info(String.format(" DataInit.HBASE_ZOOKEEPER_QUORUM:[%s]", DataInit.HBASE_ZOOKEEPER_QUORUM));

            String timeout = new String(properties.getProperty("HBASE_ZOOKEEPER_TIMEOUT").getBytes("ISO8859-1"), "utf-8");
            DataInit.HBASE_ZOOKEEPER_TIMEOUT = Integer.parseInt(timeout);
            logger.info(String.format("HBASE_ZOOKEEPER_TIMEOUT:[%s]", DataInit.HBASE_ZOOKEEPER_TIMEOUT));

            DataInit.HBASE_MASTER = new String(properties.getProperty("HBASE_MASTER").getBytes("ISO8859-1"), "utf-8");
            logger.info(String.format("DataInit.HBASE_MASTER:[%s]", DataInit.HBASE_MASTER));

            DataInit.HBASE_PORT = new String(properties.getProperty("HBASE_PORT").getBytes("ISO8859-1"), "utf-8");
            logger.info(String.format("DataInit.HBASE_PORT:[%s]", DataInit.HBASE_PORT));

            DataInit.HADOOP_HOME_DIR = new String(properties.getProperty("HADOOP_HOME_DIR").getBytes("ISO8859-1"), "utf-8");
            logger.info(String.format("HADOOP_HOME_DIR:[%s]", DataInit.HADOOP_HOME_DIR));

            logger.info("加载Hbase基础配置信息完成！");

            DataInit dataInit = new DataInit();
            dataInit.loadProperty();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
