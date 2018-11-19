package com.kr;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.BiffException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ExcelConvertCSV {

    private static final Logger logger = LoggerFactory.getLogger(ExcelConvertCSV.class);

    public static String exceltoCSV(String path) {

        long start = System.currentTimeMillis();
        String buffer = "";
        File file = null;
        try {
            file = new File(path);
            logger.info("设置读文件编码");
            WorkbookSettings setEncode = new WorkbookSettings();
            setEncode.setEncoding("GB2312");

            logger.info("从文件流中获取Excel工作区对象（WorkBook）");
            Workbook wb = Workbook.getWorkbook(file, setEncode);
            Sheet sheet = wb.getSheet(0);
            for (int i = 0; i < sheet.getRows(); i++) {
                for (int j = 0; j < 18; j++) {
                    Cell cell = sheet.getCell(j, i);
                    buffer += cell.getContents().replaceAll("\n", " ") + ",";
                }
                buffer = buffer.substring(0, buffer.lastIndexOf(",")).toString();
                buffer += "\n";
            }
        } catch (BiffException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("write the string into the file!");
        String savePath = "F:\\csv\\" + File.separator + file.getName().replace("xls", "csv");
        File saveCSV = new File(savePath);
        try {
            if (!saveCSV.exists()) {
                saveCSV.createNewFile();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(saveCSV));
            writer.write(new String(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}));
            writer.write(buffer);
            writer.flush();
            writer.close();


        } catch (IOException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        logger.info((end - start) / 1000 + "ms");

        return savePath;
    }


    public static void main(String[] args) {
        String path = "C:\\Users\\Administrator\\Desktop\\test\\13470849876姚远.xls";
        exceltoCSV(path);
    }
}

