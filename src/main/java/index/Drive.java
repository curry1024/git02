package index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class  Drive {
    /**
     * 主类
     * @param object 主类
     * @param mymap map类
     * @param mymapkey map输入key
     * @param mymapvalue map输出value
     * @param args1 FileInputFormat输入路径
     * @param args2 FileOutputFormat输出路径
     * @param num reduce个数
     * @param args3 加载缓存的路径
     *            * */
    public static void run(Class<?> object, Class<? extends Mapper> mymap, Class<?> mymapkey, Class<?> mymapvalue, int num, String args1, String args2, String args3) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(object);

        // 3 关联map和reduce
        job.setMapperClass(mymap);
        // 4 设置最终输出类型
        job.setMapOutputKeyClass(mymapkey);
        job.setMapOutputValueClass(mymapvalue);

        //缓存小表的数据
        job.addCacheArchive(new URI(args3));

        // 设置reducetask个数为0
        job.setNumReduceTasks(num);

        //判断输出路径是否存在
        Path path = new Path(args2);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args1));
        FileOutputFormat.setOutputPath(job, new Path(args2));

        // 6 提交
        job.waitForCompletion(true);
    }


    /**
     * 主类
     * @param object 主类
     * @param mymap map类
     * @param mymapkey map输入key
     * @param mymapvalue map输出value
     * @param args1 输入路径
     * @param args2 输出路径
     * @param myreduce reduce
     * @param myreducekey reduce-key
     * @param myreducevalue reduce-value
     * */
    public static void run(Class<?> object, Class<? extends Mapper> mymap, Class<?> mymapkey, Class<?> mymapvalue, Class<? extends Reducer> myreduce, Class<?> myreducekey, Class<?> myreducevalue, String args1, String args2) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(object);

        // 3 关联map和reduce
        job.setMapperClass(mymap);
        // 4 设置最终输出类型
        job.setMapOutputKeyClass(mymapkey);
        job.setMapOutputValueClass(mymapvalue);

        // 设置reduce
        job.setReducerClass(myreduce);
        job.setOutputKeyClass(myreducekey);
        job.setOutputValueClass(myreducevalue);

        //判断输出路径是否存在
        Path path = new Path(args2);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args1));
        FileOutputFormat.setOutputPath(job, new Path(args2));

        // 6 提交
        job.waitForCompletion(true);
    }
}