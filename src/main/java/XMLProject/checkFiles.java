package XMLProject;

import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class checkFiles {
    /**
     * 文件夹目录：dataDir
     * 当前文件夹下所有文件：fileList
     * 数据文件路径：filePath
     *
     * @param date
     */
    public static String dataDir = initTableInfo.srcDataDir;
    public static File[] fileList;
    public static File filePath;
    public static String flgInfo = null;
    public static List<String> fileNames = new ArrayList<String>();

    public static BufferedReader input;
    public static StringBuffer buffer = new StringBuffer();

    public static FileSystem fs;

    //获取当前文件夹下的所有文件名
    public static void getAllFileName(String date) {

        filePath = new File(dataDir + date);
        boolean datflg = false;
        if (filePath.isDirectory()) {
            fileList = filePath.listFiles();
            if (fileList.length == 0) {
                dataHandleMain.logger.error("文件目录:" + dataDir + date + " 下没有数据文件");
                System.exit(1);
            }
            // 解压zip文件
//			for (File file : fileList) {
//				if (file.getName().substring(file.getName().length()-4).equals(".zip")){
//					String unzipCode = "unzip " + dataDir + date + "/" + file.getName()
//					+ " " + dataDir + date + "/";
//					try {
//						Runtime.getRuntime().exec(unzipCode);
//					} catch (IOException e) {
//						dataHandleMain.logger.error("解压zip文件失败:" + dataDir + date + "/" + file.getName()
//						+ "   \n异常信息" + e);
//						System.exit(1);
//					}
//				}
//			}
//			for (File file : fileList) {
//				if (file.getName().substring(file.getName().length()-4).equals(".dat")){
//					datflg = true;
//					addFileNameToList(file);
//				}
//			}
//			if (!datflg) {
//				dataHandleMain.logger.error("文件目录下:" + dataDir + date + " 无.dat文件.");
//				System.exit(1);
//			}


        } else {
            dataHandleMain.logger.error("文件目录不存在:" + dataDir + date);
            System.exit(1);
        }
    }

    // 检查.dat文件与.flg文件信息是否匹配
    public static void checkInfos(String datafileName, String flgFileName, File dataFile) {
        try {
            input = new BufferedReader(new FileReader(new File(filePath.toString() + "/" + flgFileName)));
            flgInfo = input.readLine();
            input.close();

            if (flgInfo == null) {
                dataHandleMain.logger.error("flg文件为空");
                System.exit(1);
            }
            if (datafileName.equals(flgInfo.split(" ")[0])
                    && String.valueOf(dataFile.length()).equals(flgInfo.split(" ")[1])) {
                dataHandleMain.logger.info("文件的大小与flg 记录的数据文件名一致");
            } else {
                dataHandleMain.logger.error("文件的大小或者文件名 记录的数据不一致:"
                        + ".dat数据文件名与大小：" + datafileName + "--" + dataFile.length()
                        + ".flg文件记录的文件名与大小：" + flgInfo.split(" ")[0] + "---" + flgInfo.split(" ")[1]);
                System.exit(1);
            }

        } catch (FileNotFoundException e) {
            dataHandleMain.logger.error("flg文件不存在" + e);
            System.exit(1);
        } catch (IOException e) {
            dataHandleMain.logger.error("读取flg文件异常" + e);
            System.exit(1);
        }
    }

    //将符合要求的数据文件名放入List集合fileNames中
    public static void addFileNameToList(File file) {
        String tbName = file.getName().substring(0, file.getName().lastIndexOf("_"));
        boolean tbflg = false;
        for (String tableName : initTableInfo.tableNames) {
            if (tbName.equals(tableName)) {
                tbflg = true;
                checkInfos(file.getName(), file.getName().replaceAll(".dat", ".flg"), file);
                // 将数据正确的文件名放入list集合 fileNames 中
                fileNames.add(file.getName());
            }
        }
        if (!tbflg) {
            dataHandleMain.logger.error("数据文件名:" + file.getName() + "所对应的表:" + tbName + " 不存在");
            System.exit(1);

        }
    }
}
