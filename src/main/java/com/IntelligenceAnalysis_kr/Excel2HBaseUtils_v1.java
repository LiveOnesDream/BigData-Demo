package com.IntelligenceAnalysis_kr;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import jxl.read.biff.BiffException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.sun.deploy.trace.Trace.flush;


/**
 * 创建HBASE表
 * 将Excel数据批量插入到HBASE
 */

public class Excel2HBaseUtils_v1 {

    public static Configuration config;

    public static String path = "C:\\Users\\Administrator\\Desktop\\科瑞\\话单2";

    static {

        config = HBaseConfiguration.create();
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "192.168.65.154");
        config.set("hbase.master", "192.168.65.154:16030");
    }

    /**
     * 创建HBASE表
     *
     * @param tableName
     */
    public static void createTable(String tableName) {
        System.out.println("开始创建表 ......");
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            // 如果存在要创建的表，那么先删除，再创建
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName + "存在,删除.....");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("cf"));
            admin.createTable(tableDescriptor);
            admin.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功 ......");
    }

    /**
     * 批量插数据
     *
     * @param tableName
     * @throws IOException
     * @throws BiffException
     */
    public static void insertData(String tableName) throws IOException, BiffException {

        long startTime = System.currentTimeMillis();
        System.out.println("开始插入数据 ......");

        Connection conn = ConnectionFactory.createConnection(config);

//        HTable table = new HTable(config, TableName.valueOf(tableName));
        BufferedMutator mutator = null;
        TableName tName = TableName.valueOf(tableName);
        BufferedMutatorParams params = new BufferedMutatorParams(tName);
        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);//设置snappy压缩
        params.writeBufferSize(5 * 1024 * 1024);
        mutator = conn.getBufferedMutator(params);

//        hcd.setInMemory(true);
//        hcd.setMaxVersions(1);//最大版本
//        table.setAutoFlush(false);
//        table.setWriteBufferSize(5 * 1024 * 1024);

        String rowkey = " ";
        Put put = null;

        List<File> fileList = getFiles(path);
        List<Put> putLists = new ArrayList<Put>();
        JSONArray jsonArray1 = new JSONArray();
        JSONObject jsonObject1 = new JSONObject();

        for (File f : fileList) {
            jsonArray1 = ExcelUtils.readExcel(f);
            for (int i = 0; i < jsonArray1.size(); i++) {
                jsonObject1 = jsonArray1.getJSONObject(i);
                int ran = new Random().nextInt(9999);
                rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(ran)) + "_"
                        + String.valueOf(jsonObject1.get("user_number")) + "_"
                        + String.valueOf(jsonObject1.get("call_date")) + "_"
                        + String.valueOf(jsonObject1.get("call_time"));
                put = new Put(rowkey.getBytes());
                for (String key : jsonObject1.keySet()) {

                    put.add("cf".getBytes(), key.getBytes(), String.valueOf(jsonObject1.get(key)).getBytes());
                    put.setDurability(Durability.SKIP_WAL);//不写进日志文件
                }
                putLists.add(put);
            }

        }
        mutator.mutate(putLists); // 数据量达到5M时会自动提交一次
        mutator.flush();
        try {
            if (mutator != null) mutator.close();  // 提交一次
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("end insert data ......");
        long endTime = System.currentTimeMillis();
        System.out.println("插入数据用时：" + (endTime - startTime) / 1000 + "s");

    }

    /**
     * 查看HBASE表 所有数据 并统计
     *
     * @param tableName
     */

    public static void getAllRecord(String tableName) {
        long startTime = System.currentTimeMillis();
        try {
            HTable table = new HTable(config, tableName);
            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
            long count = 0;
            for (Result r : rs) {
                for (KeyValue kv : r.raw()) {
                    System.out.println("rowkey:\t" + new String(kv.getRow())
                            + " \t" + "列族:\t" + new String(kv.getFamily())
                            + " \t" + "限定词:\t" + new String(kv.getQualifier())
                            + " \t" + "value:\t" + new String(kv.getValue()));

                }
                count++;
            }
            System.out.println("总条数:" + count);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("查询数据用时：" + (endTime - startTime) / 1000 + "ms");
    }

    /**
     * 获取文件夹所有文件
     *
     * @param path 路径
     * @return
     */
    public static ArrayList<File> getFiles(String path) {
        ArrayList<File> files = new ArrayList<>();
        File file = new File(path);
        File[] tempList = file.listFiles();

        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                //              System.out.println("文件：" + tempList[i]);
                files.add(new File(tempList[i].toString()));
            }
            if (tempList[i].isDirectory()) {
                //              System.out.println("文件夹：" + tempList[i]);
            }
        }
        return files;
    }

    /**
     * 测试类
     *
     * @param args
     * @throws IOException
     * @throws BiffException
     */
    public static void main(String[] args) throws IOException, BiffException {
        String tablename = "ticket_t3";
        //       createTable(tablename);
//        insertData(tablename);
               getAllRecord(tablename);
        //       create 'ticket_t3', 'cf', {NUMREGIONS => 8, SPLITALGO => 'HexStringSplit'}

//        https://blog.csdn.net/u011192458/article/details/81221656
        // 20个表 59s 15000
        // 20个表  63s   3*1024*1024


    }
}

