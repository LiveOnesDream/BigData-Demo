package com.IntelligenceAnalysis_kr;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kr.ExcelUtil;
import jxl.read.biff.BiffException;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.Cell;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.kr.ExcelUtil.getCellValue;
import static com.kr.billExcel.newPath;


public class ExcelUtils {

    public static JSONArray readExcel(File file) throws IOException, BiffException {
        JSONArray result = new JSONArray();
        //创建输入流，读取Excel
        BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
        HSSFWorkbook wb = new HSSFWorkbook(is);
        //获取sheet对象
        HSSFSheet sheet = wb.getSheetAt(0);
        HSSFRow rootrow = sheet.getRow(0);
        //获取标题
        List<String> list = new ArrayList<>();
        for (int j = 0; j < rootrow.getLastCellNum(); j++) {
            HSSFCell hcell = rootrow.getCell(j);
            if (j == 0) {
                hcell.setCellValue("record_type_one");
            }
            if (j == 1) {
                hcell.setCellValue("record_type_two");
            }
            if (j == 2) {
                hcell.setCellValue("call_date");
            }
            if (j == 3) {
                hcell.setCellValue("call_time");
            }
            if (j == 4) {
                hcell.setCellValue("call_duration");//通话时间
            }
            if (j == 5) {
                hcell.setCellValue("event_type_one");
            }
            if (j == 6) {
                hcell.setCellValue("event_type_two");
            }
            if (j == 7) {
                hcell.setCellValue("user_number");
            }
            if (j == 8) {
                hcell.setCellValue("user_affiliation");
            }
            if (j == 9) {
                hcell.setCellValue("imsi");
            }
            if (j == 10) {
                hcell.setCellValue("imei");
            }
            if (j == 11) {
                hcell.setCellValue("other_number");
            }
            if (j == 12) {
                hcell.setCellValue("other_affiliation");
            }
            if (j == 13) {
                hcell.setCellValue("lai");
            }
            if (j == 14) {
                hcell.setCellValue("ci");
            }
            if (j == 15) {
                hcell.setCellValue("station_address_one");
            }
            if (j == 16) {
                hcell.setCellValue("station_address_two");
            }
            if (j == 17) {
                hcell.setCellValue("call_address");
            }
            String cellkey = hcell.getStringCellValue();
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
        wb.close();
        return result;
    }

    /**
     * 获取cell
     *
     * @param cell
     * @return
     */
    public static String getCellValue(Cell cell) {
        if (cell == null) {
            return "";
        }
        String value = "";
        // 以下是判断数据的类型
        switch (cell.getCellType()) {
            case HSSFCell.CELL_TYPE_NUMERIC:
                value = cell.getNumericCellValue() + "";
                if (HSSFDateUtil.isCellDateFormatted(cell)) {
                    Date date = cell.getDateCellValue();
                    if (date != null) {
                        value = new SimpleDateFormat("yyyy-MM-dd").format(date);
                    } else {
                        value = "";
                    }
                } else {
                    value = new DecimalFormat("0").format(cell.getNumericCellValue());
                }
                break;
            case HSSFCell.CELL_TYPE_STRING: // 字符串
                value = cell.getStringCellValue();
                break;
            case HSSFCell.CELL_TYPE_BOOLEAN: // Boolean
                value = cell.getBooleanCellValue() + "";
                break;
            case HSSFCell.CELL_TYPE_FORMULA: // 公式
                value = cell.getCellFormula() + "";
                break;
            case HSSFCell.CELL_TYPE_BLANK: // 空值
                value = "";
                break;
            case HSSFCell.CELL_TYPE_ERROR: // 故障
                value = "非法字符";
                break;
            default:
                value = "未知类型";
                break;
        }
        return value;
    }


    /**
     * 测试类
     *
     * @param args
     * @throws IOException
     * @throws BiffException
     */
    public static void main(String[] args) throws IOException, BiffException {

        String path = " ";
        JSONArray jsonArray = new JSONArray();
        jsonArray = ExcelUtils.readExcel(new File(path));
        readExcel(new File(newPath));
    }

}
