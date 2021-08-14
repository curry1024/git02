package index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Inverted--a.txt	3
 Inverted--b.txt	1
 Inverted--c.txt	3
 * */
class Index2Map extends Mapper<LongWritable, Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String line = value.toString();
        //切分“--”
        String[] split = line.split("--");
        k.set(split[0]);
        v.set(split[1]);
        context.write(k,v);
    }
}
