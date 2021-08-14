package index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        args = new String[]{"D:\\data\\index\\input","D:\\data\\index\\output1","D:\\data\\index\\output2"};

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);

        job1.setMapperClass(Index1Map.class);
        job1.setReducerClass(Index1Reduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        //判断输出路径是否存在
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, path);


        Job job2 = Job.getInstance(conf);

        job2.setMapperClass(Index2Map.class);
        job2.setReducerClass(Index2Reduce.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //判断输出路径是否存在
        Path path1 = new Path(args[2]);
        if(fs.exists(path1)) {
            fs.delete(path1, true);
        }
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, path1);

        //分别创建两个controlledJob对象，处理两个mapreduce程序。
        ControlledJob ajob = new ControlledJob(job1.getConfiguration());
        ControlledJob bjob = new ControlledJob(job2.getConfiguration());
        //创建一个管理组control，用于管理创建的controlledJob对象,自定义组名
        JobControl control = new JobControl("plus");
        //两个任务的关联方式
        bjob.addDependingJob(ajob);
        //addJob方法添加进组
        control.addJob(ajob);
        control.addJob(bjob);
        //设置线程对象来启动job。通过start方法。
        Thread thread = new Thread(control);
        thread.start();


/*往往会出现job线程还在执行，而main线程已经结束。
因此我们需要加上下面这一行代码，通过判断job线程是否执行完毕，
来决定是否退出jvm。通常job线程执行时间较长，
因此我们让当前线程（main线程）在发现job线程没结束的情况下，稍微等他一秒钟

 */
        while(!control.allFinished()){
            Thread.sleep(1000);
        }
        System.exit(0);
    }

}
