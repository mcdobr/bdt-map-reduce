package me.mircea.bdt;

import me.mircea.bdt.crawl.SequentialCrawler;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@SpringBootApplication
public class Application implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Value("${crawler.filesystem.target.path}")
    private String defaultFilesystemTargetPath;

    @Value("${crawler.seeds}")
    private String[] defaultSeeds;

    @Value("${crawler.page.limit}")
    private long defaultPageLimit;

    @Override
    public void run(String... args) throws Exception {
        List<HttpUrl> seedUrls = Arrays.stream(defaultSeeds)
                .map(HttpUrl::get)
                .collect(Collectors.toUnmodifiableList());

        SequentialCrawler sequentialCrawler = SequentialCrawler.builder()
                .httpClient(new OkHttpClient())
                .crawlFrontier(new ConcurrentLinkedQueue<>(seedUrls))
                .visited(new LinkedHashSet<>())
                .pageLimit(defaultPageLimit)
                .fileSystemTargetPath(Path.of(URI.create(defaultFilesystemTargetPath)))
                .build();

        sequentialCrawler.start();
    }
}
