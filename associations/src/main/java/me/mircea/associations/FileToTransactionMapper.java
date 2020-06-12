package me.mircea.associations;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileToLineMapper extends Mapper<Object, Text, IntWritable, ArrayPrimitiveWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        Map<Integer, int[]> transactionDictionary = value.toString().lines()
                .collect(Collectors.toMap(line -> atomicInteger.getAndIncrement(), this::splitLineIntoPrimitiveArray));

        for (Map.Entry<Integer, int[]> transactionEntry : transactionDictionary.entrySet()) {
            final Integer transactionId = transactionEntry.getKey();
            final int[] transactionItems = transactionEntry.getValue();

            context.write(new IntWritable(transactionId), new ArrayPrimitiveWritable(transactionItems));
        }
    }

    private int[] splitLineIntoPrimitiveArray(String line) {
        return Arrays.stream(line.split(" "))
                .mapToInt(Integer::parseInt)
                .toArray();
    }
}
