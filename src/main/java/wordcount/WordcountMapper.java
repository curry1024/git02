package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN,KEYIN,KEYOUT,KEYOUT  ---->   in K , in V  ,  out K ,out V
 * @author yqz
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //1 将maptask传给我们的文本内容先转换成String
        String line = value.toString();
        // 2 根据空格将这一行切分成单词
        // I,wish,to,wish
        String[] words = line.split(" ");
        // 3 将单词输出为<单词，1>
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
