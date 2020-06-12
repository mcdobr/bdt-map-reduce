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
        Job extractTransactionsJob = Job.getInstance(extractTransactionsConfig, "Extract transactions");
        extractTransactionsJob.setJarByClass(Associations.class);
        extractTransactionsJob.setMapperClass(FileToTransactionMapper.class);
        extractTransactionsJob.setOutputKeyClass(IntWritable.class);
        extractTransactionsJob.setOutputValueClass(ItemSetWritable.class);
        // We don't need any reducers for this map only job
        extractTransactionsJob.setNumReduceTasks(0);

        FileInputFormat.addInputPath(extractTransactionsJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(extractTransactionsJob, new Path(EXTRACT_TRANSACTIONS_PATH));
        System.exit(extractTransactionsJob.waitForCompletion(true) ? 0 : 1);

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "");
        job.setJarByClass(Associations.class);
        job.setMapperClass(ItemSetMapper.class);
//        job.setCombinerClass();
//        job.setReducerClass();

        job.setOutputKeyClass(ArrayPrimitiveWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(extractTransactionsJob, new Path(EXTRACT_TRANSACTIONS_PATH));
        FileOutputFormat.setOutputPath(extractTransactionsJob, new Path(args[1]));
        System.exit(extractTransactionsJob.waitForCompletion(true) ? 0 : 1);
    }
}
