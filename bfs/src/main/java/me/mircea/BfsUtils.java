package me.mircea;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BfsUtils {
    public static final long INFINITY = Long.MAX_VALUE >> 1;

    /**
     * fastest way to code this but not the most elegant (should have a custom writable structure).
     */
    public static Text serializeNodeStructure(long distance, Iterable<LongWritable> neighbors) {
        Stream<LongWritable> distanceWritableStream = Stream.of(new LongWritable(Math.min(distance, INFINITY)));
        Stream<LongWritable> linksStream = StreamSupport.stream(neighbors.spliterator(), false);
        return new Text(
                Stream.concat(distanceWritableStream, linksStream)
                        .mapToLong(LongWritable::get)
                        .distinct()
                        .mapToObj(Long::toString)
                        .collect(Collectors.joining(","))
        );
    }

    public static Map.Entry<Long, Iterable<Long>> deserializeNodeStructure(Text text) {
        List<Long> values = Arrays.stream(text.toString().split(","))
                .map(Long::parseLong)
                .collect(Collectors.toList());

        Long distance = values.get(0);
        List<Long> adjacency = values.subList(1, values.size());

        return new AbstractMap.SimpleImmutableEntry<>(distance, adjacency);
    }

    private BfsUtils() {
    }
}
