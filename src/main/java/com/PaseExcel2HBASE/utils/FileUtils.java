package com.PaseExcel2HBASE.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.*;

public class FileUtils {

    /**
     * 将一条数据写入文件
     * @param line 数据
     * @param append 是否追加数据
     * @return flag 写入是否成功
     */
    public static boolean writeLineToFile(String line, File file, boolean append){

        boolean flag = true;
        try {
            FileWriter fw = new FileWriter(file,append);
            line = new String(line.getBytes("GBK"),"UTF-8");
            fw.write(line + "\r\n");
            fw.flush();
            fw.close();
        }catch (Exception e){
            e.printStackTrace();
            flag = false;
        }
        return flag;
    }

    public static List<Object> readFile(File file){
        List<Object> conList = new ArrayList<Object>();
        if (file.exists()){
            BufferedReader br = null;
            try {
                InputStreamReader isReader = new InputStreamReader(new FileInputStream(file),"UTF-8");
                //为了提高效率，加入缓存技术，将字符流对象传递缓存对象的构造函数
                br = new BufferedReader(isReader,(int)file.length());
                String line = null;
                while ((line = br.readLine()) != null){//每次读取一行
                    if (StringUtils.isNotBlank(line)){
                        conList.add(line);
                    }
                }
                isReader.close();
                br.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return conList;
    }

    /**
     * 创建文件
     * @param path
     * @return
     * */
    public static File creatNewFile(String path){
        File file = null;
        boolean flag = false;
        if (StringUtils.isNotBlank(path)){
            file = new File(path);
            if (!file.exists()){
                if (file.isDirectory()){
                    return null;
                }
                try {
                    flag = file.createNewFile();
                    if (flag){
                        return file;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return file;
    }

    /**
     * 将一条数据写入文件
     * @param line 数据
     * @param filepath 文件路径
     * @param append 是否追加数据
     */
    public static boolean writeLineToFile(String line,String filepath,boolean append){
        File file = new File(filepath);
        return writeLineToFile(line,file,append);
    }

    /**
     * 将一条数据写入文件
     * @param lines 数据
     * @param filepath 文件路径
     * @param append 是否追加数据
     */
    public static boolean writeLinesToFile(String[] lines,String filepath,boolean append){
        boolean flag = true;
        try {
            File file = new File(filepath);
            if (!file.exists()){
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file,append);
            for (String line:lines){
                line = new String(line.getBytes(),"UTF-8")+"\n";
                fw.write(line);
            }
            fw.flush();
            fw.close();
        }catch (Exception e){
            e.printStackTrace();
            flag = false;
        }
        return flag;
    }


    /**
     * 若文件夹或文件不存在，则创建文件夹或文件
     * @param dirpath	目录路径
     * @return flag		目录创建是否成功的标志
     */
    public static boolean createDirOrFile(String dirpath){
        boolean flag = false;
        if (StringUtils.isNotBlank(dirpath)){
            File file = new File(dirpath);
            if (!file.exists()){
                if (file.isDirectory()){
                    file.mkdirs();
                    flag = true;
                }
                if (file.isFile()){
                    try {
                        file.createNewFile();
                        flag = true;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return flag;
    }

    /**
     * 删除单个文件
     * @param filepath : 被删除文件的文件名
     * @return		   : 单个文件删除成功返回true，否则返回false
     */
    public static boolean deleteFile(String filepath){
        boolean flag = false;
        // 路径为文件且不为空时，删除
        try {
            if (StringUtils.isBlank(filepath)){
                return flag;
            }
            File file = new File(filepath);
            if(file.exists() && file.isFile()){
                file.delete();
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 删除目录（文件夹）以及目录下的文件
     * @param path	   : 被删除目录的文件路径
     * @return		   : 目录删除成功返回true，否则返回false
     */
    private static boolean deleteDirectory(String path){
        boolean flag = false;
        if (StringUtils.isBlank(path)){
            return flag;
        }
        //如果 path 不以文件分隔符结尾，自动添加文件分隔符
        if(!path.endsWith(File.separator)){
            path +=File.separator;
        }
        File dirFile = new File(path);
        // 如果 dir 对应的文件不存在，或者不是一个目录，则退出
        if (!dirFile.exists() || !dirFile.isDirectory()){
            return flag;
        }
        // 删除文件夹下的所有文件(包括子目录)
        File[] files = dirFile.listFiles();
        for (int i = 0 ;i < files.length;i++){
            // 删除子文件
            if (files[i].isFile()){
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag){
                    break;
                }
            }else {
                flag = deleteDirectory(files[i].getAbsolutePath());
                if (!flag){
                    break;
                }
            }
        }
        if(!flag){
            return false;
        }
        // 删除当前目录
        if(dirFile.delete()){
            return true;
        } else{
            return false;
        }
    }

    /**
     * 通用的文件夹或文件删除方法，直接调用此方法，即可实现删除文件夹或文件，包括文件夹下的所有
     * 文件
     * @param path	   : 要删除的目录或文件
     * @return		   : 删除成功返回 true，否则返回 false。
     */
    public static boolean deleteFolder(String path){
        boolean flag = false;
        File file = new File(path);
        // 判断文件或目录是否存在
        if(!file.exists()){
            return flag;
        } else{
            // 判断是否为文件
            if(file.isFile()){		// 为文件时调用删除文件的方法
                return deleteFile(path);
            } else{					// 为目录时调用删除目录方法
                return deleteDirectory(path);
            }
        }
    }

    /**
     * 关闭输入输出流
     * @param is
     * @param os
     */
    public static void closeStream(InputStream is, OutputStream os){
        if (is!=null){
            try {
                is.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        if (os != null){
            try{
                os.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 打开指定路径的磁盘窗口
     * @param path 文件路径 路径必须是形如 C:\Test\ 的形式，必须是右斜杠
     */
    public static void openWindow(String path){
        Runtime runtime = Runtime.getRuntime();
        try {
            Process process = runtime.exec("cmd /c start explorer " + path);
            int exitCode = process.waitFor();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 创建一个空文件
     * @param pathname
     */
    public static void createEmpFile (String pathname) {
        File file = new File(pathname);
        if (file.exists()) {
            deleteFile(pathname);
        }

        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件判断
     * @param file
     * @return
     * */
    public static boolean checkFile(File file){
        if (!file.exists()){
            return false;
        }
        if (file == null || file.length() <=0){
            return false;
        }
        return true;

    }

    /**
     * @param data
     * @param path
     * @param code
     * @return
     * */
    public static boolean writeFile(byte[] data,String path,String code){
        boolean flag = true;
        OutputStreamWriter osw = null;
        try {
            File file = new File(path);
            if (!file.exists()){
                file = new File(file.getParent());
                if (!file.exists()){
                    file.mkdirs();
                }
            }
            if("ascii".equals(code)){
                code = "GBK";
            }
            osw = new OutputStreamWriter(new FileOutputStream(path,true),code);
            osw.write(new String(data,code));
            osw.flush();
        }catch (Exception e){
            e.printStackTrace();
            flag = false;
        }finally{
            try{
                if(osw != null){
                    osw.close();
                }
            }catch(IOException e){
                e.printStackTrace();

                flag = false;
            }
        }
        return flag;
    }
    /**
     * @param data
     * @param path
     * @param code
     * @return
     * */
    public static boolean writeFile(byte[] data, String path, String code, boolean append){
        boolean flag = true;
        OutputStreamWriter osw = null;
        try {
            File file = new File(path);
            if (!file.exists()){
                file = new File(file.getParent());
                if (!file.exists()){
                    file.mkdirs();
                }
            }
            if("ascii".equals(code)){
                code = "UTF-8";
            }
            osw = new OutputStreamWriter(new FileOutputStream(path, append), code);
            osw.write(new String(data, code));
            osw.flush();
        }catch (Exception e){
            e.printStackTrace();
            flag = false;
        }finally{
            try{
                if(osw != null){
                    osw.close();
                }
            }catch(IOException e){
                e.printStackTrace();
                flag = false;
            }
        }
        return flag;
    }

    /**
     * 获取文件名 ----- aaa.txt>>>aaa
     //* @param fileName
     * @return
     */
    /*public static String getFileName(String fileName){
        fileName = fileName.substring(fileName.lastIndexOf("\\")+1);
        return fileName.substring(0,fileName.lastIndexOf("."));
    }

    public static String getFileName(File file){
        return getFileName(file.getName());
    }*/

    public static File[] searchFile(File folder, final String keyWord) {// 递归查找包含关键字的文件
        File[] subFolders = folder.listFiles(new FileFilter() {// 运用内部匿名类获得文件
            @Override
            public boolean accept(File pathname) {// 实现FileFilter类的accept方法
                if (pathname.isDirectory()  || (pathname.isFile() && pathname.getName().toLowerCase().contains(keyWord.toLowerCase())))// 目录或文件包含关键字
                    return true;
                return false;
            }
        });

        List<File> result = new ArrayList<File>();// 声明一个集合
        if(subFolders != null) {
            for (int i = 0; i < subFolders.length; i++) {// 循环显示文件夹或文件
                if (subFolders[i].isFile()) {// 如果是文件则将文件添加到结果列表中
                    result.add(subFolders[i]);
                } else {// 如果是文件夹，则递归调用本方法，然后把所有的文件加到结果列表中
                    File[] foldResult = searchFile(subFolders[i], keyWord);
                    for (int j = 0; j < foldResult.length; j++) {// 循环显示文件
                        result.add(foldResult[j]);// 文件保存到集合中
                    }
                }
            }
        }

        File files[] = new File[result.size()];// 声明文件数组，长度为集合的长度
        result.toArray(files);// 集合数组化
        return files;
    }

    //按照文件名称排序
    public static List<File> orderByName(File [] fs) {
        List<File> files = Arrays.asList(fs);
        Collections.sort(files, new Comparator< File>() {
            @Override
            public int compare(File o1, File o2) {
                if (o1.isDirectory() && o2.isFile())
                    return -1;
                if (o1.isFile() && o2.isDirectory())
                    return 1;
                return o1.getName().compareTo(o2.getName());
            }
        });
        return files;

    }



}
