package com.kr;

import com.alibaba.fastjson.JSONObject;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.Row;

import java.io.*;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;


import static com.kr.ExcelUtil.getCellValue;
import static org.apache.poi.ss.usermodel.Cell.CELL_TYPE_NUMERIC;
import static org.apache.poi.ss.usermodel.Cell.CELL_TYPE_STRING;

public class test {

    public static void main(String[] args) throws ParseException, IOException {

        String path = "C:\\Users\\Administrator\\Desktop\\科瑞\\话单\\98767中国移动.xls";
        File f = new File(path);
        InputStream inputStream = new FileInputStream(f);
        HSSFWorkbook xssfWorkbook = new HSSFWorkbook(inputStream);
//  XSSFSheet xssfSheet = xssfWorkbook.getSheetAt(0); //如果是.xlsx文件使用这个
        HSSFSheet sheet1 = xssfWorkbook.getSheetAt(0);

        for (Row row : sheet1) {
            for (Cell hssfCell : row) {
                if (hssfCell.getCellType() == hssfCell.CELL_TYPE_BOOLEAN) {
                } else if (hssfCell.getCellType() == hssfCell.CELL_TYPE_NUMERIC) {
//                    hssfCell.setCellValue("nihao");
                } else if (hssfCell.getCellType() == Cell.CELL_TYPE_STRING) {
                    String str = hssfCell.getStringCellValue();
                    if (str.equals("recode_type")) {
                        hssfCell.setCellValue("Yes");
                    } else if (str.equals("Block")) {
                        hssfCell.setCellValue("NA");
                    } else if (str.equals("Warn")) {
                        hssfCell.setCellValue("NO");
                    }

                }
            }
        }
        FileOutputStream excelFileOutPutStream = new FileOutputStream(path);
        xssfWorkbook.write(excelFileOutPutStream);
        xssfWorkbook.close();

    }

    /**
     * 日期格式字符串转换成时间戳
     *
     * @param date_str 字符串日期
     * @param format   如：yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String date2TimeStamp(String date_str, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(date_str).getTime() / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}