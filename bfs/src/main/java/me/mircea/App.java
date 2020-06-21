package me.mircea;

import me.mircea.mapper.BfsMapper;
import me.mircea.mapper.LinkIdentityMapper;
import me.mircea.reducer.AdjacencyListReducer;
import me.mircea.reducer.BfsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static final String INTERM = "adjacency";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration adjacencyExtractorConfig = new Configuration();
        Job extractAdjacencyJob = Job.getInstance(adjacencyExtractorConfig);
        extractAdjacencyJob.setJarByClass(App.class);
        extractAdjacencyJob.setMapperClass(LinkIdentityMapper.class);
        extractAdjacencyJob.setReducerClass(AdjacencyListReducer.class);

        FileInputFormat.addInputPath(extractAdjacencyJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(extractAdjacencyJob, new Path(INTERM));
        extractAdjacencyJob.waitForCompletion(true);

        Configuration bfsConfig = new Configuration();
        Job bfsJob = Job.getInstance(bfsConfig);
        bfsJob.setJarByClass(App.class);
        bfsJob.setMapperClass(BfsMapper.class);
        bfsJob.setReducerClass(BfsReducer.class);

        FileInputFormat.addInputPath(bfsJob, new Path(INTERM));
        FileOutputFormat.setOutputPath(bfsJob, new Path(args[1]));
    }
}
