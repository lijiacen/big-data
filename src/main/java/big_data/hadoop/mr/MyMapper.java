package big_data.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Logger log = LoggerFactory.getLogger(MyReducer.class);
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

            log.info("k2:" + k2 + ",v2:" + v2);
            context.write(k2, v2);
        }
    }
}
