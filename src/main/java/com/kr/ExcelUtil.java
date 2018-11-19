package com.kr;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class ExcelUtil {

    //private static final Logger log = LoggerFactory.getLogger(ExcelUtil.class);

    private static final String SUFFIX_2007 = "xls";
    private static final String SUFFIX_2013 = "xlsx";


    /**
     * 读取excel表中的数据
     * <p>
     * map的key为工作表名称，list为工作表中列名和数据的键值对
     *
     * @param file
     * @return
     */
    public static Map<String, List<Map<String, Object>>> getTableSheetData(File file) {
        if (!file.exists()) {
            return null;
        }
        return getExcelData(file);
    }

    /**
     * 读取excel表的表头，验证excel模板
     *
     * @param file
     * @return
     */
    public static List<String> getTableSheetDeader(File file) {
        if (!file.exists()) {
            return null;
        }
        List<String> headerList = new ArrayList();
        Row headerRow = getTableSheetDeaderRow(file);
        for (int i = 0; i < headerRow.getLastCellNum(); i++) {
            headerList.add(headerRow.getCell(i).toString());
        }
        return headerList;
    }

    public static boolean isExcelFile(String fileName) {
        return fileName.endsWith(SUFFIX_2007) || fileName.endsWith(SUFFIX_2013);
    }

    private static Row getTableSheetDeaderRow(File file) {
        try {
            Workbook workbook = WorkbookFactory.create(file);
            if (null == workbook) {
                //log.error("未知文件格式");
                return null;
            }
            //获取工作表数量
            int sheetNumber = workbook.getNumberOfSheets();
            for (int i = 0; i < sheetNumber; i++) {
                Sheet sheet = workbook.getSheetAt(i);
                return sheet.getRow(0);
            }
        } catch (Exception e) {
            //log.error("解析文件出错", e);
        }
        return null;
    }

    private static Map<String, List<Map<String, Object>>> getExcelData(File file) {
        Workbook workbook = null;
        try {
            workbook = WorkbookFactory.create(file);
            if (null == workbook) {
                //log.error("未知文件格式");
                return null;
            }
            Map<String, List<Map<String, Object>>> result = new HashMap<String, List<Map<String, Object>>>();
            //获取工作表数量
            int sheetNumber = workbook.getNumberOfSheets();
            for (int i = 0; i < sheetNumber; i++) {
                List<Map<String, Object>> sheet_data = new ArrayList<Map<String, Object>>();
                Sheet sheet = workbook.getSheetAt(i);
                if (ifSheetNullOrEmpty(sheet)) {
                    continue;
                }
                //工作表名称
                String sheet_name = workbook.getSheetName(i);
                int num = sheet.getLastRowNum();
                //log.info("工作表" + sheet_name + " 数据有" + num + "行");
                for (int j = 1; j <= num; j++) {
                    Row row = sheet.getRow(j);
                    if (ifRowNullOrEmpty(row)) {
                        continue;
                    }
                    Map<String, Object> record = new HashMap<String, Object>();
                    //获取表头
                    Row first = sheet.getRow(0);
                    getRowData(row, record, first);
                    sheet_data.add(record);
                }
                result.put(sheet_name, sheet_data);
            }
            return result;
        } catch (Exception e) {
            //log.error("解析文件出错", e);
        } finally {
            if (null != workbook) {
                try {
                    workbook.close();
                } catch (IOException e) {
                    //log.error("解析文件出错", e);
                }
            }
        }
        return null;
    }

    /**
     * 此处有个点要注意,getLastCellNum,下标是从1开始,有多少列,这里就是这个值.而getLastRowNum,下标是从0开始,也就是21行的表格,这里获得的值是20.用户可自行验证.
     *
     * @param row    该行记录
     * @param record 返回值
     * @param first  表头
     */
    private static void getRowData(Row row, Map<String, Object> record, Row first) {
        /*for (int k = 0; k < row.getLastCellNum(); k++) {
            String value = "";
            Cell cell = row.getCell(k);
            CellType cellType = cell.getCellTypeEnum();
            if (cellType.equals(CellType.BOOLEAN)) {
                value = cell.getBooleanCellValue() + "";
            } else if (cellType.equals(CellType.NUMERIC)) {
                value = cell.getNumericCellValue() + "";
            } else if (cellType.equals(CellType.STRING)) {
                value = cell.getStringCellValue();
            }
            record.put(first.getCell(k).toString(), value.trim());
        }*/
    }

    public static boolean ifRowNullOrEmpty(Row row) {
        if (row == null || row.getLastCellNum() == 0 || row.getCell(0) == null) {
            return true;
        }
        //遍历每个元素，如果都为空，返回false
        int length = row.getLastCellNum();
        //记录改行表格内容为空的总量
        int count = 0;
        for (int i = 0; i < length; i++) {
            Cell cell = row.getCell(i);
            if (null == cell || "".equals(cell.toString())) {
                count++;
            }
        }
        //如果该行表格中列内容为空的数量等于该行的列的长度，则说明该行为空
        if (count == length) {
            return true;
        }
        return false;
    }

    public static boolean ifSheetNullOrEmpty(Sheet sheet) {
        if (sheet == null || sheet.getLastRowNum() == 0) {
            return true;
        }
        return false;
    }

    private static boolean validExcelFormat(String fileName, String type) {
        if (getOS().contains("win")) {
            return getFileNameSuffix(fileName).equalsIgnoreCase(type);
        } else if (getOS().contains("linux")) {
            return getFileNameSuffix(fileName).equals(type);
        } else {
            return false;
        }
    }

    private static String getFileNameSuffix(String fileName) {
        return fileName.substring(fileName.lastIndexOf("." + 1, fileName.length()));
    }


    private static String getOS() {
        return System.getProperty("os.name").toLowerCase();
    }

    public static void main(String[] args) {
        File file = new File("C:\\Users\\Administrator\\Desktop\\科瑞\\WorkaAndProblem\\98767中国移动.xls");
        List<String> tableSheetDeader = getTableSheetDeader(file);
//        Map<String, List<Map<String, Object>>> result = getTableSheetData(file);

        //log.info("Map<String, List<Map<String, Object>>>Map<String, List<Map<String, Object>>>");
        Map<String, List<Map<String, Object>>> result = getExcelData(file);
        System.out.println(result);
    }

    //读取并判断类型
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
     * 通过file读取excel并返回数据
     *
     * @param file
     * @return
     */
    public static JSONArray readExcel(File file) {
        JSONArray resultList = new JSONArray();
        try {
            // 创建输入流，读取Excel
            InputStream is = new FileInputStream(file);
            HSSFWorkbook wb = new HSSFWorkbook(is);
            // 每个页签创建一个Sheet对象
            HSSFSheet sheet = wb.getSheetAt(0);

            //获取标题名称集
            List<String> list = new ArrayList();
            HSSFRow rootRow = sheet.getRow(0);

            for (int r = 0; r < rootRow.getLastCellNum(); r++) {
                HSSFCell cell = rootRow.getCell(r);
                String cellValue = getCellValue(cell);
                list.add(cellValue);
            }
            //获取内容
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                JSONObject jsonObject = new JSONObject();
                // row.getLastCellNum()返回该页的总列数
                HSSFRow row = sheet.getRow(i);
                for (int j = 0; j < row.getLastCellNum(); j++) {
                    HSSFCell cell = row.getCell(j);
                    //将获取得值放入map集合
                    jsonObject.put(list.get(j), getCellValue(cell));
                }
                resultList.add(jsonObject);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

    //    public static void main(String[] args) {
//        File file = new File("C:\\Users\\Administrator\\Desktop\\科瑞\\WorkaAndProblem\\98767中国移动.xls");
//        List<String> list = getTableSheetDeader(file);
//        for (String str : list) {
//            System.out.println(str);
//        }
//    }
    public static void readData(String path) {
        File file = new File(path);

    }


}

