package com.kr;

import com.IntelligenceAnalysis_kr.ExcelUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import jxl.read.biff.BiffException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static com.kr.ExcelUtil.getCellValue;

public class billExcel {

    public static String newPath = " ";

    /**
     * @param path
     * @return
     * @throws IOException
     */

    public static ArrayList<String> updataExcel(String path) throws IOException {

        ArrayList<String> files = new ArrayList<>();
        File file = new File(path);
        File[] tempList = file.listFiles();
        for (int i = 0; i < tempList.length; i++) {

            if (tempList[i].isFile()) {

                String xlsName = tempList[i].getName();
                InputStream is = new FileInputStream(tempList[i]);
                HSSFWorkbook wb = new HSSFWorkbook(is);
                HSSFSheet sheet = wb.getSheetAt(0);
                HSSFRow hssfRow = sheet.getRow(0);

                for (int j = 0; j < hssfRow.getLastCellNum(); j++) {
                    HSSFCell hcell = hssfRow.getCell(j);
                    if (j == 0) {
                        hcell.setCellValue("recode_code");
                    }
                    if (j == 1) {
                        hcell.setCellValue("recode_type");
                    }
                    if (j == 2) {
                        hcell.setCellValue("call_date");
                    }
                    if (j == 3) {
                        hcell.setCellValue("call_time");
                    }
                    if (j == 4) {
                        hcell.setCellValue("event_code");
                    }
                    if (j == 5) {
                        hcell.setCellValue("event_type");
                    }
                    if (j == 6) {
                        hcell.setCellValue("event_typeb");
                    }
                    if (j == 7) {
                        hcell.setCellValue("subscriber_numbe");
                    }
                    if (j == 8) {
                        hcell.setCellValue("affiliation");
                    }
                    if (j == 9) {
                        hcell.setCellValue("IMSI");
                    }
                    if (j == 10) {
                        hcell.setCellValue("IMEI");
                    }
                    if (j == 11) {
                        hcell.setCellValue("tell_num");
                    }
                    if (j == 12) {
                        hcell.setCellValue("Anothers_place");
                    }
                    if (j == 13) {
                        hcell.setCellValue("LAI");
                    }
                    if (j == 14) {
                        hcell.setCellValue("CI");
                    }
                    if (j == 15) {
                        hcell.setCellValue("base_address");
                    }
                    if (j == 16) {
                        hcell.setCellValue("base_addressb");
                    }
                    if (j == 17) {
                        hcell.setCellValue("call_area_code");
                    }


                }
                newPath = "D:\\话单\\";
                wb.write(new File(newPath + File.pathSeparator + xlsName));

                files.add(newPath);
            }
//            https://zhidao.baidu.com/question/1642180420766689340.html
        }
        return files;

    }

    public static JSONArray readExcel(File file) throws IOException, BiffException {
        boolean isSuccess = true;
        JSONArray result = new JSONArray();
        //创建输入流，读取Excel
        InputStream is = new FileInputStream(file);
        POIFSFileSystem fs = new POIFSFileSystem(is);

        HSSFWorkbook wb = new HSSFWorkbook(fs);
        //获取sheet对象
        HSSFSheet sheet = wb.getSheetAt(0);

        //获取标题
        List<String> list = new ArrayList<>();
        HSSFRow rootrow = sheet.getRow(0);
        for (int i = 0; i < rootrow.getLastCellNum(); i++) {
            HSSFCell cell = rootrow.getCell(i);
            String cellkey = cell.getStringCellValue();
            list.add(cellkey);
        }

        //获取内容
        for (int j = 1; j < sheet.getLastRowNum(); j++) {
            JSONObject jsonObject = new JSONObject();
            HSSFRow row = sheet.getRow(j);
            for (int i = 0; i < row.getLastCellNum(); i++) {
                HSSFCell cell = row.getCell(i);
                String cellvalue = getCellValue(cell);
                jsonObject.put(list.get(i), getCellValue(cell));
            }
            result.add(jsonObject);
        }
        return result;
    }

    public static void main(String[] args) {
        String oldpath = "D:\\账单\\";
        try {
            ArrayList<File> fileList = getFiles(oldpath);
            for (int j = 0; j < fileList.size(); j++) {
                File file = (File) fileList.get(j);
                JSONArray jsonArray = ExcelUtils.readExcel(file);
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    for (String key : jsonObject.keySet()) {
                        System.out.println(key + " : " + jsonObject.get(key));
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (BiffException e) {
            e.printStackTrace();
        }
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
}


