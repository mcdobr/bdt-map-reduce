package me.mircea.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LinkIdentityMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
    @Override
    protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
