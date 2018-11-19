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

public class hbaseOp03 {

    public static String path = "C:\\Users\\Administrator\\Desktop\\科瑞\\话单2";

    public static void main(String[] args) throws IOException, BiffException {
        long startTime = System.currentTimeMillis();
        //获取陪着参数
        Configuration config = HBaseConfiguration.create();
        System.setProperty("hadoop.home.dir", "F:\\hadoop-eclipse-plugin");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", "192.168.65.154");
        config.set("hbase.master", "192.168.65.154:16030");
        //建立连接
        Connection conn = ConnectionFactory.createConnection(config);

        try {
            //连接表 获取表对象
//            Table table = conn.getTable(TableName.valueOf("Ticket_splited4"));
            BufferedMutator mutator = null;
            TableName tableName = TableName.valueOf("Ticket_splited4");
            BufferedMutatorParams params = new BufferedMutatorParams(tableName);
            params.writeBufferSize(5 * 1024 * 1024);

            try {
                mutator = conn.getBufferedMutator(params);
                List<File> fileList = getFiles(path);
                List<Put> putLists = new ArrayList<Put>();
                JSONArray jsonArray1 = new JSONArray();
                JSONObject jsonObject1 = new JSONObject();

                String rowkey = " ";
                Put put = null;
                for (File f : fileList) {
                    jsonArray1 = ExcelUtils.readExcel(f);
                    for (int i = 0; i < jsonArray1.size(); i++) {
                        jsonObject1 = jsonArray1.getJSONObject(i);
                        int ran = new Random().nextInt(999);
                        rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(ran)) + String.valueOf(jsonObject1.get("user_number")) + "_"
                                + String.valueOf(jsonObject1.get("call_date")) + String.valueOf(jsonObject1.get("call_time"));
                        put = new Put(rowkey.getBytes());

                        for (String key : jsonObject1.keySet()) {
                            String value = (String) jsonObject1.get(key);
                            put.add("f1".getBytes(), key.getBytes(), value.getBytes());
                            put.setDurability(Durability.SKIP_WAL);//不写进日志文件
                        }
                        putLists.add(put);
                    }

                    mutator.mutate(putLists);
                    mutator.flush();
                }
            } finally {
                if (mutator != null) mutator.close();
            }
        } finally {
            conn.close();
        }
        System.out.println("end insert data ......");
        long endTime = System.currentTimeMillis();
        System.out.println("插入数据用时：" + (endTime - startTime)/1000  + "s");
    }


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
}