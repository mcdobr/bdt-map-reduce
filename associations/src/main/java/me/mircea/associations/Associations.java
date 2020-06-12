package me.mircea.associations;

import me.mircea.associations.mapper.ItemSetMapper;
import me.mircea.associations.reducer.ThresholdSupportReducer;
import me.mircea.associations.writable.ItemSetWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

public class Associations {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration extractSubsetFrequencyConfig = new Configuration();
        Job subsetFrequencyJob = Job.getInstance(extractSubsetFrequencyConfig, "Subset_frequency");
        subsetFrequencyJob.setJarByClass(Associations.class);
        subsetFrequencyJob.setMapperClass(ItemSetMapper.class);
        subsetFrequencyJob.setCombinerClass(IntSumReducer.class);
        subsetFrequencyJob.setReducerClass(ThresholdSupportReducer.class);

        subsetFrequencyJob.setOutputKeyClass(ItemSetWritable.class);
        subsetFrequencyJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(subsetFrequencyJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(subsetFrequencyJob, new Path(args[1]));
        subsetFrequencyJob.waitForCompletion(true);
    }
}
