package big_data.hadoop.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** 小文件解决方案MapFile */
public class SmallFileMap {
    /**
     * 生成squence文件
     *
     * @param inputFilePath 输入目录(本地)
     * @param outputFileDir hdfs上的输出文件夹
     */
    private static void write(String inputFilePath, String outputFileDir) {
        if (outputFileDir == null || inputFilePath == null)
            throw new IllegalArgumentException("参数异常");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://1.15.61.173:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");

        // 如果输出文件夹已存在，则删除
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            boolean flag = fileSystem.delete(new Path(outputFileDir), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 构造opt数组，三个元素（输出路径，key类型，value类型）
        SequenceFile.Writer.Option[] opts =
                new SequenceFile.Writer.Option[] {
                    MapFile.Writer.keyClass(Text.class), MapFile.Writer.valueClass(Text.class)
                };

        MapFile.Writer writer = null;
        try {
            writer = new MapFile.Writer(conf, new Path(outputFileDir), opts);
            // 指定文件需要压缩的目录
            File filePath = new File(inputFilePath);
            if (filePath.isDirectory()) {
                File[] files = filePath.listFiles();
                // 在代码中对获取到的文件名进行排序即可(否则有坑)
                // 对获取到的文件进行排序
                List<File> fileList = Arrays.asList(files);
                Collections.sort(
                        fileList,
                        new Comparator<File>() {
                            @Override
                            public int compare(File f1, File f2) {
                                return f1.getName().compareTo(f2.getName());
                            }
                        });

                for (File file : files) {
                    try {
                        // 获取文件全部内容
                        String content = FileUtils.readFileToString(file, "UTF-8");
                        String fileName = file.getName();
                        Text key = new Text(fileName);
                        Text value = new Text(content);
                        // 向SquenceFile中写入数据
                        writer.append(key, value);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * 读取hdfs上的squence文件
     *
     * @param filePath hdfs上的文件路径
     */
    private static void reader(String filePath) {

        if (filePath == null) throw new IllegalArgumentException("参数异常");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://1.15.61.173:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");

        MapFile.Reader reader = null;
        try {
            reader = new MapFile.Reader(new Path(filePath), conf);
            Text key = new Text();
            Text value = new Text();
            while (reader.next(key, value)) {
                System.out.println(key.toString() + "" + value.toString());
            }

        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        // write("/Users/lijiacen/Test", "/mapFile");
        reader("/mapFile");
    }
}
