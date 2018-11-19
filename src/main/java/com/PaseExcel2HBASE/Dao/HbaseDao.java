package com.PaseExcel2HBASE.Dao;

import com.PaseExcel2HBASE.initialize.DataInit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HbaseDao {

    protected static Connection connection;
    public static Admin admin;
    private static Logger logger = LoggerFactory.getLogger(HbaseDao.class);

    /**
     * 静态构造，在调用静态方法时前进行运行
     * 初始化连接对象．
     */
    static void getConn() {

        Configuration configuration = HBaseConfiguration.create();
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        configuration.set("hbase.zookeeper.quorum", DataInit.HBASE_ZOOKEEPER_QUORUM);
        configuration.setInt("hbase.rpc.timeout", DataInit.HBASE_ZOOKEEPER_TIMEOUT);
        configuration.setInt("hbase.client.operation.timeout", DataInit.HBASE_ZOOKEEPER_TIMEOUT);
        configuration.set("hbase.zookeeper.property.clientPort", DataInit.HBASE_PORT);
        configuration.set("hbase.master", DataInit.HBASE_MASTER);

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建HBASE表
     *
     * @param tablName
     * @throws IOException
     */
    public void createTab(String tablName) throws IOException {
        long startTime = System.currentTimeMillis();
        getConn();
        TableName tableName = TableName.valueOf(tablName);
        if (admin.tableExists(tableName) != false) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            logger.info(tablName + " 存在，删除...");
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(DataInit.HBASE_FAMILYNAME);
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
        logger.info("表创建成功", tableName);

    }

    /**
     * 批量插数据到HBASE
     *
     * @param tablName
     * @throws IOException
     */

    public void Insert2HBase(String tablName, List<Put> putLists) throws IOException {

        long startTime = System.currentTimeMillis();
        getConn();
        TableName tableName = TableName.valueOf(tablName);
        if (admin.tableExists(tableName) != false) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            logger.info(tablName + " 存在，删除...");
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(DataInit.HBASE_FAMILYNAME);
        hTableDescriptor.addFamily(hColumnDescriptor);
//        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
//        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        admin.createTable(hTableDescriptor);
        logger.info("表创建成功", tableName);
        Put put = null;
        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(tablName));
            hTable.setWriteBufferSize(5 * 1024 * 1024);
            hTable.setAutoFlush(false, true);
            if (putLists.size() == 5000) {
                hTable.put(putLists);
                hTable.flushCommits();
                putLists.clear();
            }
            hTable.put(putLists);
            hTable.flushCommits();

            logger.info("end insert data ......");
            logger.info("插入数据用时：" + (System.currentTimeMillis() - startTime) / 1000 + "s");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            logger.info("关闭admin异常");
            e.printStackTrace();
        }

    }


}
/**
 * protected static Connection connection;
 * public static Admin admin;
 * private static Logger logger = LoggerFactory.getLogger(BatchInsertData.class);
 * static void getConn(){
 * <p>
 * Configuration configuration=HBaseConfiguration.create();
 * System.setProperty("hadoop.home.dir","F:\\hadoop-eclipse-plugin");
 * configuration.set("hbase.zookeeper.quorum",DataInit.HBASE_ZOOKEEPER_QUORUM);
 * configuration.setInt("hbase.rpc.timeout",DataInit.HBASE_ZOOKEEPER_TIMEOUT);
 * configuration.setInt("hbase.client.operation.timeout",DataInit.HBASE_ZOOKEEPER_TIMEOUT);
 * configuration.set("hbase.zookeeper.property.clientPort",DataInit.HBASE_PORT);
 * configuration.set("hbase.master",DataInit.HBASE_MASTER);
 * <p>
 * try{
 * connection=ConnectionFactory.createConnection(configuration);
 * admin=connection.getAdmin();
 * }catch(IOException e){
 * e.printStackTrace();
 * }
 * }
 *
 * @Override public void Insert2HBase(String tablName) throws IOException {
 * //tablName = "huada";
 * long startTime = System.currentTimeMillis();
 * getConn();
 * TableName tableName = TableName.valueOf(tablName);
 * if (admin.tableExists(tableName) != false) {
 * admin.disableTable(tableName);
 * admin.deleteTable(tableName);
 * logger.info(tablName + " 存在，删除...");
 * }
 * HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
 * HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(DataInit.HBASE_FAMILYNAME);
 * <p>
 * hTableDescriptor.addFamily(hColumnDescriptor);
 * admin.createTable(hTableDescriptor);
 * logger.info("表创建成功", tableName);
 * <p>
 * //        try {
 * //            Class clazz = DataInit.parseModule;
 * //            Object obj = clazz.newInstance();
 * //            Method getDataFractionation = clazz.getMethod("getDataFractionation",);
 * //            getDataFractionation.invoke(obj);
 * //
 * //        } catch (IllegalAccessException e) {
 * //            e.printStackTrace();
 * //        } catch (InvocationTargetException e) {
 * //            e.printStackTrace();
 * //        } catch (NoSuchMethodException e) {
 * //            e.printStackTrace();
 * //        } catch (InstantiationException e) {
 * //            e.printStackTrace();
 * //        }
 * String rowkey = " "; //设计rowkey
 * Put put = null;
 * <p>
 * try {
 * HTable hTable = (HTable) connection.getTable(TableName.valueOf(tablName));
 * List<Put> putLists = new ArrayList<Put>();
 * JSONObject jsonObject = new JSONObject();
 * billService billS = new billService();
 * DataInit.jsonArray = billS.readExcel(DataInit.excelFile);
 * for (int i = 0; i < DataInit.jsonArray.size(); i++) {
 * jsonObject = DataInit.jsonArray.getJSONObject(i);
 * int ran = new Random().nextInt(999);
 * <p>
 * if ("huada".equals(tablName)) {
 * rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(ran))
 * + String.valueOf(jsonObject.get("user_number")) + "_"
 * + String.valueOf(jsonObject.get("call_date")) + "_"
 * + String.valueOf(jsonObject.get("call_time"));
 * } else if ("".equals(tablName)) {
 * rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(ran))
 * + String.valueOf(jsonObject.get("user_number")) + "_"
 * + String.valueOf(jsonObject.get("call_date")) + "_"
 * + String.valueOf(jsonObject.get("call_time"));
 * }
 * <p>
 * put = new Put(rowkey.getBytes());
 * <p>
 * for (String key : jsonObject.keySet()) {
 * String value = (String) jsonObject.get(key);
 * put.add("cf".getBytes(), key.getBytes(), value.getBytes());
 * put.setDurability(Durability.SKIP_WAL);//不写进日志文件
 * <p>
 * }
 * putLists.add(put);
 * <p>
 * if (putLists.size() == 15000) {
 * hTable.put(putLists);
 * hTable.flushCommits();
 * putLists.clear();
 * logger.info("inserting  data ... ");
 * }
 * }
 * <p>
 * hTable.put(putLists);
 * hTable.flushCommits();
 * logger.info("end insert data ......");
 * logger.info("插入数据用时：" + (System.currentTimeMillis() - startTime) / 1000 + "s");
 * <p>
 * } catch (Exception e) {
 * e.printStackTrace();
 * } finally {
 * close();
 * }
 * }
 */