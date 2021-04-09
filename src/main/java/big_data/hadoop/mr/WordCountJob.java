package big_data.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/** 读取HDFS上的文件 */
public class WordCountJob {
    /** map阶段 */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        /**
         * 需要实现map函数 接收k1,v1，产生k2,v2
         *
         * @param k1
         * @param v1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Context context)
                throws IOException, InterruptedException {
            // k1 代表每行行首的偏移量, v1代表每一行内容
            // 对获取到的每一行数据切割，把单词切割出来
            String[] words = v1.toString().split(" ");
            // 迭代切割出的单词数据
            for (String word : words) {
                // 把迭代的单词封装成<k2,v2>
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                context.write(k2, v2);
            }
        }
    }

    /** reduce阶段 */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        /**
         * 针对<k2,{v2...}>的数据进行求和，并转化为k3,v3写出去
         *
         * @param k2
         * @param v2s
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
                throws IOException, InterruptedException {
            // 创建sum变量保存v2s的和
            long sum = 0;
            for (LongWritable v2 : v2s) {
                sum += v2.get();
            }

            // 组装k3,v3
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            context.write(k3, v3);
        }
    }

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

            // 指定reduce相关代码
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

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
