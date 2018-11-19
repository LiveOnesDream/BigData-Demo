package XMLProject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class dataHandleMapper extends Mapper<LongWritable, Text, Text, Text> {
    // key
    int dataNum = 0;
    private String cutLength = " ";
    private int sumLength = 0;
    //	private Pattern chinesePattern = Pattern.compile("[\u4E00-\u9FA5]+");
//	private Matcher matcherResult = null;
    //长度为一的字符串
    private String oneStr = "";
    // linevalue 的长度
    private int lvLength = 0;
    // 记录错误的数据条数
    static int erroNum = 0;
    private final Logger logger = Logger.getLogger(dataHandleMapper.class);

    // 初始化
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        cutLength = context.getConfiguration().get("fieldLength");
        // 计算数据每行总长度
        for (String fieldLength : cutLength.split(":")) {
            sumLength += Integer.parseInt(fieldLength);
        }
        super.setup(context);
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws

            UnsupportedEncodingException {

        dataNum += 1;
        // 定义截取的字符串
        StringBuffer cutStr = new StringBuffer();
        // map端读取每一行
        if (sumLength != value.getBytes().length) {

            erroNum += 1;
            logger.error("数据第" + dataNum + "有问题：" + "实际字节长度为 " + value.getBytes().length
                    + "记录字节长度为 " + sumLength);
//			System.exit(1);
        }
        String linevalue = new String(value.getBytes(), "GBK");
        // linevalue 的长度
        lvLength = linevalue.length();
        // 处理每一行的数据，根据获取定长的长度去截取数据然后写入文件
        if (sumLength == lvLength) {
            // 该行数据中不含有中文
            for (String fieldLength : cutLength.split(":")) {
                if (linevalue.length() == Integer.parseInt(fieldLength)) {
                    cutStr.append(linevalue.substring(0, Integer.parseInt(fieldLength)).trim());
                } else {
                    cutStr.append(linevalue.substring(0, Integer.parseInt(fieldLength)).trim() + "\u0001");
                }
                linevalue = linevalue.substring(Integer.parseInt(fieldLength));
            }
        } else {
            // 该行数据中含有中文
            for (int i = 0; i < lvLength; i++) {
                oneStr = linevalue.substring(0, 1);
                if (oneStr.getBytes().length > 1) {
                    cutStr.append(oneStr + " ");
                    linevalue = linevalue.substring(1);
                } else {
                    cutStr.append(oneStr);
                    linevalue = linevalue.substring(1);
                }
            }

            // 开始分割处理后的该行数据
            linevalue = cutStr.toString();
            // 清空 cutStr
            cutStr.setLength(0);
            if (sumLength == linevalue.length()) {
                for (String fieldLength : cutLength.split(":")) {
                    if (linevalue.length() == Integer.parseInt(fieldLength)) {
                        cutStr.append(linevalue.substring(0, Integer.parseInt(fieldLength)).replaceAll(" ", ""));
                    } else {
                        cutStr.append(linevalue.substring(0, Integer.parseInt(fieldLength)).replaceAll(" ", "") + "\u0001");
                    }
                    linevalue = linevalue.substring(Integer.parseInt(fieldLength));
                }
            }
        }

        // 输出
        try {

            context.write(new Text(String.valueOf(dataNum)), new Text(cutStr.toString()));
//			context.write(new Text(String.valueOf(dataNum)), new Text(String.valueOf(lvLength)));

        } catch (IOException | InterruptedException e) {
            System.out.println("map端输出出错：" + e);
        }

    }
}
