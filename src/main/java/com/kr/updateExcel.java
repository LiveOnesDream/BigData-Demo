package com.kr;

import com.sun.imageio.plugins.wbmp.WBMPImageReader;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.NotOLE2FileException;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;

import java.io.*;
import java.nio.file.FileSystem;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class updateExcel {

    public static String path = "C:\\Users\\Administrator\\Desktop\\科瑞\\金寨2018吸毒人员2\\";

    static String selectsql = null;
    static ResultSet retsult = null;

    public static final String url = "jdbc:mysql://192.168.13.204/jdyp_depot";
    public static final String name = "com.mysql.jdbc.Driver";
    public static final String user = "jdyp";
    public static final String password = "keyway123";

    public static Connection conn = null;
    public static PreparedStatement pst = null;

    /**
     * excel文件添加字段
     *
     * @param file
     */
    public static void excelAddField(File file) throws IOException {

        //创建输入流，读取Excel
        BufferedInputStream is = null;
        List<File> errFile = new ArrayList<>();
        try {
            is = new BufferedInputStream(new FileInputStream(file));
            HSSFWorkbook wb = new HSSFWorkbook(is);

            //获取sheet对象
            HSSFSheet sheet = wb.getSheetAt(0);
            HSSFRow rootrow = sheet.getRow(0);
            HashMap<String, List> map = readmysql();

            rootrow.createCell(18).setCellValue("station_place");
            rootrow.createCell(19).setCellValue("longitude");
            rootrow.createCell(20).setCellValue("latitude");

            int index = 1;
            while (index < sheet.getLastRowNum()) {

                HSSFRow cell = sheet.getRow(index);
                List station_place = map.get("station_place");
                List longitude = map.get("longitude");
                List latitude = map.get("latitude");

                cell.createCell(18).setCellValue((String) station_place.get(index));
                cell.createCell(19).setCellValue((String) longitude.get(index));
                cell.createCell(20).setCellValue((String) latitude.get(index));

                index++;
            }

            String path = "C:\\Users\\Administrator\\Desktop\\科瑞\\金寨2018吸毒人员第二批" + File.separator + file.getName();

            File newfile = new File(path);
//            if (newfile.exists()) {
//                newfile.deleteOnExit();
//                newfile = new File(path);
//            }
            FileOutputStream fo = new FileOutputStream(newfile);
            wb.write(fo);
            fo.flush();
            wb.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexOutOfBoundsException e) {

            System.out.println(file + "有异常");
            String errpath ="C:\\Users\\Administrator\\Desktop\\问题文件"+File.separator+file.getName();
            FileOutputStream fis = new FileOutputStream(errpath);
            fis.flush();
            fis.close();
        } catch (OfficeXmlFileException e) {
            System.out.println(file + "问题文件");
        }

    }


    /**
     * 获取mysql数据
     *
     * @return
     */
    public static HashMap<String, List> readmysql() {

        HashMap<String, List> map = new HashMap<>();
        selectsql = "select longitude,latitude,station_place from base_station_jz";//SQL语句
        try {
            Class.forName(name);//指定连接类型
            conn = DriverManager.getConnection(url, user, password);//获取连接
            pst = conn.prepareStatement(selectsql);//准备执行语句
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            retsult = pst.executeQuery();//执行语句，得到结果集
            List<String> longitude = new ArrayList<>();
            List<String> latitude = new ArrayList<>();
            List<String> station_place = new ArrayList<>();
            while (retsult.next()) {

                longitude.add(retsult.getString("longitude"));
                latitude.add(retsult.getString("latitude"));
                station_place.add(retsult.getString("station_place"));

                map.put("longitude", longitude);
                map.put("latitude", latitude);
                map.put("station_place", station_place);

            }

            retsult.close();
            conn.close();//关闭连接
            pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    public static void main(String[] args) {

        List<File> fileList = getFiles(path);
        for (File file : fileList) {
            try {
                excelAddField(file);
            } catch (NotOLE2FileException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static ArrayList<File> getFiles(String path) {

        ArrayList<File> files = new ArrayList<>();
        File file = new File(path);
        File[] tempList = file.listFiles();

        for (int i = 0; i < tempList.length; i++) {
            if (tempList[i].isFile()) {
                System.out.println("文件：" + tempList[i]);
                files.add(new File(tempList[i].toString()));
            }
            if (tempList[i].isDirectory()) {
                System.out.println("文件夹：" + tempList[i]);
            }
        }
        return files;
    }
}
