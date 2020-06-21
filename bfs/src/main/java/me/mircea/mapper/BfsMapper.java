package me.mircea.mapper;

import me.mircea.BfsUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BfsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    /**
     * todo: how do i not hardcode the source?
     */
    private static final long SOURCE = 25L;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final Map.Entry<Long, Iterable<Long>> distanceAndAdjacency = BfsUtils.deserializeNodeStructure(value);
        final Long distance = (key.get() == SOURCE) ? 0L : distanceAndAdjacency.getKey();
        final List<Long> neighbors = StreamSupport.stream(distanceAndAdjacency.getValue().spliterator(), false)
                .collect(Collectors.toList());

        context.write(key, BfsUtils.serializeNodeStructure(distance, neighbors.stream().map(LongWritable::new).collect(Collectors.toList())));
        for (Long neighbor : neighbors) {
            long neighborDistance = Math.min(1 + distance, BfsUtils.INFINITY);
            context.write(new LongWritable(neighbor), BfsUtils.serializeNodeStructure(neighborDistance, Collections.emptyList()));
        }
    }
}
