package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 相当于一个yarn集群的客户端，
 * 需要在此封装我们的mr程序相关运行参数，指定jar包
 * 最后提交给yarn
 *
 * @author Administrator
 */
public class WordcountDriver {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
//集群打包用动态变量
       // args = new String[]{"D:\\data\\splitsdata", "D:\\data\\splitsdata01"};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordcountDriver.class);
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //将多个文件划分到一个mapper完成如果不设置InputFormat，它默认用的是TextInputFormat.class
      job.setInputFormatClass(CombineTextInputFormat.class);
       CombineTextInputFormat.setMaxInputSplitSize(job,3*1024*1024);
       CombineTextInputFormat.setMinInputSplitSize(job,2*1024*1024);
      //  job.setInputFormatClass(CombineTextInputFormat.class);
      // CombineTextInputFormat.setMaxInputSplitSize(job,128*1024*1024);
     //   CombineTextInputFormat.setMinInputSplitSize(job,2*1024*1024);


        //combiner
        //job.setCombinerClass(WordcountCombiner.class);
        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);
    /* CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
      CombineTextInputFormat.setMinInputSplitSize(job, 2097152);*/

        //设置分区2个
     job.setPartitionerClass(WordCountPartitioner.class);
     //reducetask只能2个，否则报错
     job.setNumReduceTasks(2);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

        long endTime = System.currentTimeMillis();

        System.out.println(endTime - startTime);
    }
}
