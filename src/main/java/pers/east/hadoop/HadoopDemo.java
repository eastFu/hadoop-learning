package pers.east.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author East.F
 * @ClassName: HadoopDemo
 * @Description: hadoop 测试
 * @date 2019/6/17 17:41
 */
public class HadoopDemo {

    /**
     * 获取HDFS集群文件系统中的文件到本地文件系统
     */
    public static  void testGetDataFromHdfs()throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://pm0400:9000/user/hadoop/mr/wordcount/input/wordcountInput"), conf);
        FSDataInputStream is = fs.open(new Path("hdfs://pm0400:9000/user/hadoop/mr/wordcount/input/wordcountInput"));
        OutputStream os=new FileOutputStream(new File("E:/a.txt"));
        byte[] buff= new byte[1024];
        int length;
        while ((length=is.read(buff))!=-1){
            System.out.println(new String(buff,0,length));
            os.write(buff,0,length);
            os.flush();
        }
        System.out.println(fs.getClass().getName());
    }


    public static void main(String[] args) throws IOException {
        testGetDataFromHdfs();

    }
}
