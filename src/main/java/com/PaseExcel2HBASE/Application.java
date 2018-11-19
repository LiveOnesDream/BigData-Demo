package com.PaseExcel2HBASE;


import com.PaseExcel2HBASE.Service.HandleFile;
import com.PaseExcel2HBASE.Service.PaseExcelService;
import com.PaseExcel2HBASE.initialize.DataInit;
import com.PaseExcel2HBASE.utils.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Application {

    static Logger logger = Logger.getLogger(Application.class);

    public static void main(String[] args) throws IOException {
        logger.info("Program startting!");

        HandleFile handleFile = new HandleFile();
        logger.info("循环遍历文件夹，便于确定解析的文件类型");
        List<File> allExcelFile = handleFile.getFiles();
        if (allExcelFile.size() > 0) {
            for (File file : allExcelFile) {
                //判断文件是否被占用，若能改名表示没有被占用，执行
                if (file.renameTo(file)) {
                    DataInit.excelFile = file;
                    PaseExcelService paseExcelService = new PaseExcelService();
                    paseExcelService.save2Hbase();
                    logger.info("所有文件写入完毕，删除原文件");
                    FileUtils.deleteFile(file.getName());
                }
            }

        }
        DataInit.dao.close();
    }
}


