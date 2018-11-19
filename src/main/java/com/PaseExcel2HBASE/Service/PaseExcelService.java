package com.PaseExcel2HBASE.Service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.PaseExcel2HBASE.Dao.HbaseDao;
import com.PaseExcel2HBASE.initialize.DataInit;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PaseExcelService {

    HbaseDao hbaseDao = new HbaseDao();
    PaseExcel paseExcel = new PaseExcel();

    public void save2Hbase() throws IOException {

        if (DataInit.excelFile.getName().contains("号码")) {
            String tbname = getString(DataInit.excelFile.getName());
            String rowkey = "";
            List<Put> putList = new ArrayList<>();
            Put put = null;
            JSONArray jsonArray = paseExcel.paseTicketExcel(DataInit.excelFile);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                rowkey = String.valueOf(jsonObject.get("user_number")) + "_"
                        + String.valueOf(jsonObject.get("call_date")) + "_"
                        + String.valueOf(jsonObject.get("call_time"));
                put = new Put(rowkey.getBytes());
                for (String key : jsonObject.keySet()) {
                    String value = (String) jsonObject.get(key);
                    put.add("cf".getBytes(), key.getBytes(), value.getBytes()).setDurability(Durability.SKIP_WAL);
                    putList.add(put);
                }

            }
            hbaseDao.Insert2HBase(tbname, putList);

        } else if (DataInit.excelFile.getName().contains("账单交易明细")) {
            String rowkey = "";
            List<Put> putList = new ArrayList<>();
            Put put = null;
            JSONArray jsonArray = paseExcel.paseBillExcel(DataInit.excelFile);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                rowkey = String.valueOf(jsonObject.get("user_number")) + "_"
                        + String.valueOf(jsonObject.get("call_date")) + "_"
                        + String.valueOf(jsonObject.get("call_time"));
                for (String key : jsonObject.keySet()) {
                    String value = (String) jsonObject.get(key);
                    put.add("cf".getBytes(), key.getBytes(), value.getBytes()).setDurability(Durability.SKIP_WAL);
                }
                putList.add(put);
            }
            hbaseDao.Insert2HBase(DataInit.excelFile.getName(), putList);
        }

    }

    public String getString(String tbname) {

        String regEx = "[^0-9]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(tbname);
        String tbName = m.replaceAll("").trim();
        return tbName;
    }
}
