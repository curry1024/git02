import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
//import org.junit.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSAPI {
    //ctlr + shift + enter
    public static void main(String[] args) {
        try {
            HDFSAPI.putFileToHDFS();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印本地hadoop地址值
     * IO的方式写代码
     */
    @Test
    public void intiHDFS() throws IOException {
        //F2 可以快速的定位错误 alt + enter自动找错误
        //1.创建配信信息对象 ctrl + alt + v  后推前  ctrl + shitl + enter 补全
        Configuration conf = new Configuration();
        //2.获取文件系统
        FileSystem fs = FileSystem.get(conf);
        //3.打印文件系统
        System.out.println(fs.toString());
    }

    /**
     * 上传代码
     */

    public static void putFileToHDFS() throws Exception {
        //注：import org.apache.hadoop.conf.Configuration;
        //ctrl + alt + v 推动出对象
        //1.创建配置信息对象
        Configuration conf = new Configuration();
        //2.设置部分参数
        conf.set("dfs.replication", "2");
        //3.找到HDFS的地址
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), conf, "root");
        //4.上传本地文件的路径
        Path src = new Path("D:/data/people.txt");
        //Path src = new Path("D:\\data\\people.txt");
        //Path src = new Path("file:///opt//module//data//people.txt");
        //5.要上传到HDFS的路径
        //Path dst = new Path("hdfs://bigdata112:9000/");
        //6、上传的路径直接可以用"/"
        Path dst = new Path("/user/people1.txt");
        //6.以拷贝的方式上传，从src -> dst
        fs.copyFromLocalFile(src, dst);
        //7.关闭
        fs.close();
        System.out.println("OK啦");
    }

    @Test
    public void getFileFromHDFS() throws Exception {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), configuration, "root");
//	fs.copyToLocalFile(new Path("hdfs://bigdata112:9000/user/itstar/hello.txt"), new Path("d:/hello.txt"));
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件效验
        // 2 下载文件
        fs.copyToLocalFile(false, new Path("hdfs://bigdata112:9000/word.txt"), new Path("d:/data/fromhdfscopy.txt"), true);
        fs.close();
    }

    @Test
    public void MkdirandDelandRenameAtHDFS() throws Exception {
        // 1 创建配置信息对象


        System.setProperty("HADOOP_USER_NAME", "root");
        String hdfsuri = "hdfs://bigdata112:9000";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsuri);
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("usr/hadoop/test"));
       /* FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), configuration, "root");
        fs.mkdirs(new Path("hdfs://bigdata112:9000/a/b/c"));*/
        fs.mkdirs(new Path("hdfs://bigdata112:9000/a1/b/c"));
        // fs.rename(new Path("hdfs://bigdata112:9000/a"),new Path("hdfs://bigdata112:9000/a3"));
        fs.delete(new Path("hdfs://bigdata112:9000/a1"), true);
        fs.close();

    }

    @Test
    public void deleteAtHDFS() throws Exception {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), configuration, "root");

        //2 删除文件夹 ，如果是非空文件夹，参数2是否递归删除，true递归
        fs.delete(new Path("hdfs://bigdata112:9000/user/itstar/output"), true);
        fs.close();
    }

    @Test
    public void putFileToHDFS1() throws Exception {// HDFS文件上传,通过IO流操作HDFS
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");
        // 2 创建输入流
        FileInputStream inStream = new FileInputStream(new File("e:/hello.txt"));
        // 3 获取输出路径
        String putFileName = "hdfs://bigdata111:9000/user/itstar/hello1.txt";
        Path writePath = new Path(putFileName);
        // 4 创建输出流
        FSDataOutputStream outStream = fs.create(writePath);
        // 5 流对接
        try {
            IOUtils.copyBytes(inStream, outStream, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }
    }

    //HDFS文件下载
    @Test
    public void getFileFromHDFS1() throws Exception {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), configuration, "root");
        // 2 获取读取文件路径
        String filename = "hdfs://bigdata112:9000/word.txt";
        // 3 创建读取path
        Path readPath = new Path(filename);
        // 4 创建输入流
        FSDataInputStream inStream = fs.open(readPath);
        // 5 流对接输出到控制台
        try {
            IOUtils.copyBytes(inStream, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inStream);
        }
    }

    @Test
// 定位下载第一块内容
    public void readFileSeek1() throws Exception {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), configuration, "root");

        // 2 获取输入流路径
        Path path = new Path("hdfs://bigdata112:9000/usr/hadoop/spark-2.1.0-bin-h27hive.tgz/");

        // 3 打开输入流
        FSDataInputStream fis = fs.open(path);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream("D:\\data\\spark-2.1.0-bin-h27hive.tgz.part1");

        // 5 流对接
        byte[] buf = new byte[1024];
        for (int i = 0; i < 128 * 1024; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        // 6 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    @Test
    // 定位下载第二块内容
    public void readFileSeek2() throws Exception {

        // 1 创建配置信息对象
        Configuration configuration = new Configuration();
//获取文件系统实例：
        // FileSystem.get(conf),返回core-site.xml--fs.default.name 默认文件系统
        //"itstar"限制了文件系统的用户，这是从安全方面考虑的
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), configuration, "itstar");

        // 2 获取输入流路径
        Path path = new Path("hdfs://bigdata112:9000/user/itstar/tmp/hadoop-2.7.2.tar.gz");

        // 3 打开输入流
        FSDataInputStream fis = fs.open(path);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream("e:/hadoop-2.7.2.tar.gz.part2");

        // 5 定位偏移量（第二块的首位）
        fis.seek(1024 * 1024 * 128);

        // 6 流对接,1024用来拷贝缓存的大小
        IOUtils.copyBytes(fis, fos, 1024);

        // 7 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    @Test
    public void readListFiles() throws Exception {
        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.链接文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), conf, "root");
        //3.迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/user"), true);
        //4.遍历迭代器
        while (listFiles.hasNext()) {
            //一个一个出
            LocatedFileStatus fileStatus = listFiles.next();
            //名字
            System.out.println("文件名：" + fileStatus.getPath().getName());
            //块大小
            System.out.println("block大小：" + fileStatus.getBlockSize());
            //权限
            System.out.println("权限：" + fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] locations = fileStatus.getBlockLocations();
            for (BlockLocation bl : locations) {
                System.out.println("block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("------------------华丽的分割线----------------");
        }
    }

    @Test
    public void IsFileHdfs() throws Exception {
        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.链接文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), conf, "root");
        FileStatus[] filestatus=fs.listStatus(new Path("/"));
        for(FileStatus status :filestatus)
        {
            if(status.isFile()){
                System.out.println(status.getPath().getName()+" is file");
            }
            else{
                System.out.println(status.getPath().getName()+"  is dir");
            }
        }

    }
    @Test
    public void create() throws Exception{
        Configuration conf = new Configuration();
        //2.链接文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata112:9000"), conf, "root");
        FSDataOutputStream out = fs.create(new Path("/hdfsapi/test/a.txt"));

  String words="hello world!!";
   out.writeBytes(words);
       // out.write(words.getBytes("UTF-8"));
       // out.writeUTF("Hello hadoop");
        out.flush();//输出走缓冲区，所以先flush
        out.close();
    }


}


