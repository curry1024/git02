package index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce
 * 获取map数据：index--a.txt 1
 * 最后结果
 * index--a.txt 2
 *
 *
 * */
class Index1Reduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException {
        //初始化计数器
        int count = 0;
        for (IntWritable iw:values){
            count+=iw.get();
        }
        v.set(count);
        //输出
        context.write(key,v);
    }
}
