package me.mircea.bdt;

import com.google.common.net.HttpHeaders;
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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@SpringBootApplication
public class Application implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Value("${filesystem.documents.path}")
    private String documentsFilesystemPath;

    @Value("${filesystem.back.links.path}")
    private String backLinksFilesystemPath;

    @Value("${crawler.seeds}")
    private String[] defaultSeeds;

    @Value("${crawler.page.limit}")
    private long defaultPageLimit;

    @Value("${crawler.user.agent}")
    private String userAgent;

    @Value("${filesystem.reverse.lookup.path}")
    private String reverseLookupFilesystemPath;

    @Override
    public void run(String... args) throws Exception {
        List<HttpUrl> seedUrls = Arrays.stream(defaultSeeds)
                .map(HttpUrl::get)
                .collect(Collectors.toUnmodifiableList());

        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addNetworkInterceptor(chain -> chain.proceed(
                        chain.request()
                                .newBuilder()
                                .header(HttpHeaders.USER_AGENT, userAgent)
                                .build()
                ))
                .build();

        SequentialCrawler sequentialCrawler = SequentialCrawler.builder()
                .httpClient(httpClient)
                .crawlFrontier(new ConcurrentLinkedQueue<>(seedUrls))
                .visited(new LinkedHashSet<>())
                .pageLimit(defaultPageLimit)
                .fileSystemDocumentsPath(Path.of(URI.create(documentsFilesystemPath)))
                .fileSystemBackLinksPath(Path.of(URI.create(backLinksFilesystemPath)))
                .fileSystemReverseLookupPath(Path.of(URI.create(reverseLookupFilesystemPath)))
                .build();

        sequentialCrawler.start();
    }
}
