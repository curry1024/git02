package index;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URISyntaxException;

public class Index1 {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        args = new String[]{"D:\\data\\index\\input",
                "D:\\data\\index\\output1"};
        Drive.run(Index1.class,Index1Map.class, Text.class,IntWritable.class,Index1Reduce.class,
                Text.class, IntWritable.class,args[0],args[1]);
    }
}
/**
 * 倒排索引map
 * 数据 index Inverted index
 *index--a.txt
 *
 * */
