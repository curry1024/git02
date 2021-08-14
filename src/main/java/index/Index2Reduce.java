package index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce
 * 获取map数据：index    [a.txt 3,b.txt	1,c.txt	3]
 * 最后结果
 *
 * index    a.txt-->3   b.txt-->1   c.txt-->3
 *
 * */
class Index2Reduce extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text t: values){
            //a.txt 3
            String[] split = t.toString().split("\t");
            //重现按照-->
            String s = split[0] + "-->" + split[1];
            sb.append(s).append("\t");
        }
        //输出
        context.write(key,new Text(sb.toString()));
    }
}

