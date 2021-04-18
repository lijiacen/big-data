package big_data.hadoop.mr;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/** 小文件解决方案SequenceFile */
public class SmallFileSeq {
    /**
     * 生成squence文件
     *
     * @param inputFilePath 输入目录(本地)
     * @param outputFilePath hdfs上的输出文件
     */
    private static void write(String inputFilePath, String outputFilePath) {
        if (outputFilePath == null || inputFilePath == null)
            throw new IllegalArgumentException("参数异常");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://1.15.61.173:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");

        // 如果输出文件夹已存在，则删除
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            boolean flag = fileSystem.delete(new Path(outputFilePath), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 构造opt数组，三个元素（输出路径，key类型，value类型）
        SequenceFile.Writer.Option[] opts =
                new SequenceFile.Writer.Option[] {
                    SequenceFile.Writer.file(new Path(outputFilePath)),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(Text.class)
                };

        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(conf, opts);
            // 指定文件需要压缩的目录
            File filePath = new File(inputFilePath);
            if (filePath.isDirectory()) {
                File[] files = filePath.listFiles();
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

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(filePath)));
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
        // write("/Users/lijiacen/Test", "/seqFile");
        reader("/seqFile");
    }
}
