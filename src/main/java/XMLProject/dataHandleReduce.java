package XMLProject;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class dataHandleReduce extends Reducer<Text, Text, NullWritable, Text> {

    protected void reduce(Text Text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // 定义一个StringBuffer
        StringBuffer strBuff = new StringBuffer();

        for (Text text : values) {

            context.write(NullWritable.get(), text);
        }

    }
}
