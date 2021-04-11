package big_data.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    Logger log = LoggerFactory.getLogger(MyReducer.class);

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
        log.info("k3:" + k3 + ",v3:" + v3);
        context.write(k3, v3);
    }
}
