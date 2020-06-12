package me.mircea.associations.reducer;

import me.mircea.associations.writable.ItemSetWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Class has same functionality as @{@link org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer}
 * but also has a threshold.
 */
public class ThresholdSupportReducer extends Reducer<ItemSetWritable, IntWritable, ItemSetWritable, IntWritable> {
    private static final int SUPPORT_THRESHOLD = 128;

    @Override
    protected void reduce(ItemSetWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        if (sum >= SUPPORT_THRESHOLD) {
            IntWritable result = new IntWritable(sum);
            context.write(key, result);
        }
    }
}
