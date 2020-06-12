package me.mircea.associations;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class FileToTransactionMapper extends Mapper<Object, Text, Text, ItemSetWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, int[]> transactionDictionary = Arrays.stream(value.toString().split("[\r\n]+"))
                .collect(Collectors.toMap(line -> UUID.randomUUID().toString(), this::splitLineIntoPrimitiveArray));

        for (Map.Entry<String, int[]> transactionEntry : transactionDictionary.entrySet()) {
            final String transactionId = transactionEntry.getKey();
            final int[] transactionItems = transactionEntry.getValue();

            context.write(new Text(transactionId), new ItemSetWritable(transactionItems));
        }
    }

    private int[] splitLineIntoPrimitiveArray(String line) {
        return Arrays.stream(line.split(" "))
                .mapToInt(Integer::parseInt)
                .toArray();
    }
}
