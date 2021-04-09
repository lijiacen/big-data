package big_data.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class HdfsVo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //        conf.set("fs.defaultFS", "hdfs://49.234.108.177:9000");
        conf.set("fs.defaultFS", "hdfs://1.15.61.173:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fileSystem = FileSystem.get(conf);
        // put(fileSystem);
        // get(fileSystem);
        // del(fileSystem);
    }

    private static void del(FileSystem fileSystem) throws IOException {
        boolean flag = fileSystem.delete(new Path("/aa.txt"), true);
    }

    private static void get(FileSystem fileSystem) throws IOException {
        FSDataInputStream fis = fileSystem.open(new Path("/aa.txt"));
        FileOutputStream fos = new FileOutputStream("D:\\bb.txt");
        IOUtils.copyBytes(fis, fos, 1024, true);
    }

    private static void put(FileSystem fileSystem) throws IOException {
        // 创建文件
        FileInputStream fis = new FileInputStream("D:\\aa.txt");
        FSDataOutputStream fos = fileSystem.create(new Path("/aa.txt"));
        IOUtils.copyBytes(fis, fos, 1024, true);
    }
}
