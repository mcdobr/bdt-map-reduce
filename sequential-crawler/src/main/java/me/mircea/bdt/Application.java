package me.mircea.bdt;

import com.google.common.net.HttpHeaders;
import me.mircea.bdt.crawl.SequentialCrawler;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
public class Application implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Value("${filesystem.target}")
    private String targetFolder;

    @Value("${crawler.seeds}")
    private String[] defaultSeeds;

    @Value("${crawler.page.limit}")
    private long defaultPageLimit;

    @Value("${crawler.user.agent}")
    private String userAgent;


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


        Path target = Path.of(URI.create(targetFolder));
        Path documentsFilesystemPath = target.resolve("documents");
        Path connectivityGraphsFilesystemPath = target.resolve("connectivity");
        Path backLinksFilesystemPath = target.resolve("back_links");
        Path reverseLookupFilesystemPath = target.resolve("reverse_lookup");

        Stream.of(target, documentsFilesystemPath, connectivityGraphsFilesystemPath, backLinksFilesystemPath, reverseLookupFilesystemPath)
                .forEach(path -> {
                    try {
                        Files.createDirectories(path);
                    } catch (IOException e) {
                        throw new IllegalStateException("Could not create required directories");
                    }
                });


        SequentialCrawler sequentialCrawler = SequentialCrawler.builder()
                .httpClient(httpClient)
                .crawlFrontier(new ConcurrentLinkedQueue<>(seedUrls))
                .visited(new LinkedHashSet<>())
                .pageLimit(defaultPageLimit)
                .fileSystemConnectivityPath(connectivityGraphsFilesystemPath)
                .fileSystemDocumentsPath(documentsFilesystemPath)
                .fileSystemBackLinksPath(backLinksFilesystemPath)
                .fileSystemReverseLookupPath(reverseLookupFilesystemPath)
                .build();

        sequentialCrawler.start();
    }
}
