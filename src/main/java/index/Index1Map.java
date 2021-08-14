package index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 倒排索引map
 * 数据 index Inverted index
 *index--a.txt
 *
 * */
class Index1Map extends Mapper<LongWritable, Text,Text, IntWritable> {


    Text k = new Text();
    IntWritable v = new IntWritable();
    String name;
    String locat;
    /**
     * 初始化获取文件的名字
     * */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        name = inputSplit.getPath().getName();
        long length = inputSplit.getLength();

        System.out.println("length====>"+" "+length);


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取数据 index Inverted index
        String line = value.toString();
        //切分数据
        String[] split = line.split(" ");
        //拼接，关键词和文件名字
        for (String s:split){
            k.set(s+"--"+name);
            v.set(1);
            context.write(k,v);
        }

    }
}
