package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yqz
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        // 1 汇总各个key的个数
        for (IntWritable value : values) {
            count += value.get();
        }
        // 2输出该key的总次数
        context.write(key, new IntWritable(count));
    }
}
