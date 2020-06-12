package me.mircea.associations;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

// Algorithm 1 in the lab paper
// todo: might need to inverse the output because keys should implement WritableComparable which ArrayPrimitiveWritable
//  does not
public class ItemSetMapper extends Mapper<IntWritable, ItemSetWritable, ArrayPrimitiveWritable, IntWritable> {
    // todo: how do i inject these values in a "hadoop way" if example codes do not instantiate mappers
    //  via constructors? maybe setup?
//    50675 39
//    42135 48
//    15596 38
//    15167 32
//    14945 41
//    4472 65
//    3837 89
//    3257 225
//    3099 170
    private static final Set<Integer> MOST_COMMON_VALUES = Sets.newHashSet(39, 48, 38, 32, 41, 65, 89, 225, 170);
    private static final Set<Set<Integer>> POWER_SET = Sets.powerSet(MOST_COMMON_VALUES);

    @Override
    protected void map(IntWritable key, ItemSetWritable value, Context context) throws IOException, InterruptedException {
        int[] rawArray = (int[]) value.get();

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
