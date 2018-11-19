package com.kr;

import com.IntelligenceAnalysis_kr.ExcelUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import jxl.read.biff.BiffException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class hbaseOp {

    public static Configuration configuration;

    public static String path = "C:\\Users\\Administrator\\Desktop\\科瑞\\话单2";

    static {
        configuration = HBaseConfiguration.create();
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.65.154");
        configuration.set("hbase.master", "192.168.65.154:16030");
    }

    //创建HBASE 表
    public static void createTable(String tableName) {
        System.out.println("开始创建表 ......");
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            // 如果存在要创建的表，那么先删除，再创建
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName + "存在,删除.....");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("cf"));
            admin.createTable(tableDescriptor);

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("创建成功 ......");
    }

    //插数据
    public static void insertData(String tableName) throws IOException, BiffException {

        long startTime = System.currentTimeMillis();
        System.out.println("开始插入数据 ......");

        HTablePool pool = new HTablePool(configuration, 1000);
        String rowkey = " ";
        Put put = null;
        JSONArray jsonArray;
        JSONObject jsonObject;

        //批量插入数据
        List<Put> putLists = new ArrayList<Put>();
        File file1 = new File(path);
        File[] files = file1.listFiles();
        for (File ff : files) {
            jsonArray = ExcelUtils.readExcel(ff);
            for (int i = 0; i < jsonArray.size(); i++) {
                jsonObject = jsonArray.getJSONObject(i);
                int ran = new Random().nextInt(99999);
//                        String test = MD5Hash.getMD5AsHex(jsonObject.getBytes("user_number"));
                rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(ran)) + String.valueOf(jsonObject.get("user_number")) + "_"
                        + String.valueOf(jsonObject.get("call_date")) + String.valueOf(jsonObject.get("call_time"));
                put = new Put(rowkey.getBytes());
                for (String key : jsonObject.keySet()) {
                    String value = (String) jsonObject.get(key);
                    put.add("f1".getBytes(), key.getBytes(), value.getBytes());
                    put.setDurability(Durability.SKIP_WAL);//不写进日志文件
                    putLists.add(put);

                }
            }
        }
        if (putLists.size() == 1000) {
            pool.getTable(tableName).put(putLists);
            putLists.clear();
            System.out.println("a");
        }
        pool.getTable(tableName).put(putLists);
        System.out.println("inserting  data ......");
        pool.close();
        System.out.println("end insert data ......");
        long endTime = System.currentTimeMillis();
        System.out.println("插入数据用时：" + (endTime - startTime) / 1000 + "ms");
    }

    //查看HBASE表数据
    public static void getAllRecord(String tableName) {
        long startTime = System.currentTimeMillis();
        try {
            HTable table = new HTable(configuration, tableName);
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

    //获取文件路径，
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

//    public static byte[][] splitkeys;
//    private static byte[][] getSplitkeys ( char startChar, String rowkey, boolean isPartition) {
//        String[] str = {"0","1", "2", "3", "4", "5", "6", "7", "8", "9", "/", "+", "=",
//                "A", "B", "C", "D", "E","F","G","H","I", "J", "K", "L", "M","N","O","P","Q", "R", "S", "T", "U","V","W","X","Y", "Z",
//                "a", "b", "c", "d", "e","f","g","h","i", "j", "k", "l", "m","n","o","p","q", "r", "s", "t", "u","v","w","x","y", "z"};
//
//        int regionCount = 18;
//        splitkeys = new byte[regionCount - 1][];
//        byte[] bytes;
//        for (int i = 0; i < str.length -1 ; i++) {
//            bytes = Bytes.toBytes( str[i] );
//            int mod = bytes[0] % regionCount;
//            splitkeys[mod - 1] = bytes;
//        }
//
//        /*for (int i = 0; i < bytes.length; i++) {
//        splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
//            System.out.println(bytes[i] + "--" + bytes[0]);
//            System.err.println(bytes[i] + "============" + bytes[0] % DataInit.regionCount);
//        }*/
//        return splitkeys;
//
//    }


    public static void main(String[] args) throws IOException, BiffException {
        String tablename = "Ticket_splited3";
//        createTable(tablename);
        insertData(tablename);
//        getAllRecord(tablename);

    }
}
