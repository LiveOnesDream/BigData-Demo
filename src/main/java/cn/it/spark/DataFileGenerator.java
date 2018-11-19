package cn.it.spark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * 伪造数据
 */

public class DataFileGenerator {
    public static void main(String[] args){
        File file = new File("C:\\Users\\Administrator\\Desktop\\DataFile.txt");
        try {
            FileWriter fileWriter = new FileWriter(file);
            Random rand = new Random();
            for (int i=1;i<=100000;i++){
                fileWriter.write(i +" " + (rand.nextInt(100)+1));
                fileWriter.write(System.getProperty("line.separator"));
            }
            fileWriter.flush();
            fileWriter.close();

        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
