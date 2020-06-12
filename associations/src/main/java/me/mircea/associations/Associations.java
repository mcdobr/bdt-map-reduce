package me.mircea.associations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Associations {
    private static final String EXTRACT_TRANSACTIONS_PATH = "/user/root/transactions";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // Extract transactions job
        Configuration extractTransactionsConfig = new Configuration();
        Job transactionJob = Job.getInstance(extractTransactionsConfig, "Extract transactions");
        transactionJob.setJarByClass(Associations.class);
        transactionJob.setMapperClass(FileToTransactionMapper.class);
        transactionJob.setOutputKeyClass(IntWritable.class);
        transactionJob.setOutputValueClass(ItemSetWritable.class);
        // We don't need any reducers for this map only job
        transactionJob.setNumReduceTasks(0);

        FileInputFormat.addInputPath(transactionJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(transactionJob, new Path(EXTRACT_TRANSACTIONS_PATH));
        transactionJob.waitForCompletion(true);

        Configuration extractSubsetFrequencyConfig = new Configuration();
        Job subsetFrequencyJob = Job.getInstance(extractSubsetFrequencyConfig, "Extract relevant subset frequency");
        subsetFrequencyJob.setJarByClass(Associations.class);
        subsetFrequencyJob.setMapperClass(ItemSetMapper.class);
//        subsetFrequencyJob.setCombinerClass();
//        subsetFrequencyJob.setReducerClass();
        subsetFrequencyJob.setOutputKeyClass(ItemSetWritable.class);
        subsetFrequencyJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(subsetFrequencyJob, new Path(EXTRACT_TRANSACTIONS_PATH));
        FileOutputFormat.setOutputPath(subsetFrequencyJob, new Path(args[1]));
        subsetFrequencyJob.waitForCompletion(true);
    }
}
