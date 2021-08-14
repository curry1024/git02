package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/*
* mapper输出后进行partition.数据来自mapper的输出结果<Text,IntWritable>
* */
public class WordCountPartitioner extends Partitioner<Text, IntWritable>{
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        // 1 获取单词key
       // String firWord = key.toString().substring(0, 1);
        // 2 根据单词长度奇数偶数分区
        String firWord = key.toString();
        int length = firWord.length();
        if (length % 2 == 0) {
            return 0;
        }else {
            return 1;
        }


        //按第一个字母进行分区*/
       /* char[] charArray = firWord.toCharArray();
        int result = charArray[0];
   return  result;*/

    }
}

