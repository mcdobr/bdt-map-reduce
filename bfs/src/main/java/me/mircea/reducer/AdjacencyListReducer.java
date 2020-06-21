package me.mircea.reducer;

import me.mircea.BfsUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AdjacencyListReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(BfsUtils.serializeNodeStructure(BfsUtils.INFINITY, values)));
    }
}
