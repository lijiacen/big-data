package big_data.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/** 只有map，无reduce */
public class WordCountJobNoReduce {

    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                throw new IllegalArgumentException("参数异常");
            }

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://1.15.61.173:9000");
            conf.set("dfs.client.use.datanode.hostname", "true");
            Job job = Job.getInstance(conf);

            job.setJarByClass(WordCountJob.class);

            // 指定输入路径（可以文件，可以目录）
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            // 输出路径（指定一个不存在的目录）
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 指定map相关代码
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // 禁用reduce
            job.setNumReduceTasks(0);

            // 提交job
            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
        }
    }
}
