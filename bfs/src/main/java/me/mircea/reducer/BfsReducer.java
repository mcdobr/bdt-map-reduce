package me.mircea.reducer;

import me.mircea.BfsUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BfsReducer extends Reducer<LongWritable, Text, LongWritable, LongWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterable<Long> neighbors;
        Long minDistance = BfsUtils.INFINITY;

        for (Text text : values) {
            Map.Entry<Long, Iterable<Long>> nodeStructure = BfsUtils.deserializeNodeStructure(text);
            Long distance = nodeStructure.getKey();

            if (hasOnlyDistance(nodeStructure)) {
                if (minDistance >= distance) {
                    minDistance = distance;
                }
            } else {
                neighbors = nodeStructure.getValue();
            }
        }
        // is this the needed output or do i need to run the job again
        context.write(key, new LongWritable(minDistance));
    }

    private boolean hasOnlyDistance(Map.Entry<Long, Iterable<Long>> nodeStructure) {
        return !nodeStructure.getValue().iterator().hasNext();
    }
}
