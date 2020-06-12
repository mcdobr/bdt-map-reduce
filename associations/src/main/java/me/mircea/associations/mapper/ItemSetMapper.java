package me.mircea.associations.mapper;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import me.mircea.associations.writable.ItemSetWritable;
import me.mircea.associations.writable.UuidWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

// Algorithm 1 in the lab paper
public class ItemSetMapper extends Mapper<UuidWritable, ItemSetWritable, ItemSetWritable, IntWritable> {
    // todo: how do i inject these values in a "hadoop way" if example codes do not instantiate mappers
    //  via constructors? maybe setup?
    private static final Set<Integer> MOST_COMMON_VALUES = Sets.newHashSet(39, 48, 38, 32, 41, 65, 89, 225, 170);
    private static final Set<Set<Integer>> POWER_SET = Sets.powerSet(MOST_COMMON_VALUES);

    @Override
    protected void map(UuidWritable key, ItemSetWritable value, Context context) throws IOException, InterruptedException {
        int[] rawArray = value.get();

        Set<Integer> items = Arrays.stream(rawArray).boxed()
                .collect(Collectors.toSet());

        Set<Set<Integer>> relevantSubsetInTransaction = POWER_SET.parallelStream()
                .filter(items::containsAll)
                .collect(Collectors.toSet());

        for (Set<Integer> subset : relevantSubsetInTransaction) {
            context.write(new ItemSetWritable(Ints.toArray(subset)), new IntWritable(1));
        }
    }
}
