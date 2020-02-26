package me.mircea.bdt.crawl;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Builder
public class SequentialCrawler {
    @NonNull
    private final ConcurrentLinkedQueue<HttpUrl> crawlFrontier;

    @NonNull
    private final Set<HttpUrl> visited;

    @NonNull
    private final OkHttpClient httpClient;

    @NonNull
    private final Path fileSystemTargetPath;

    @NonNull
    private final UUID uuid = UUID.randomUUID();

    private final long pageLimit;

    public void start() {
        log.info("Started crawl job {} with crawl frontier: {}", uuid, crawlFrontier);

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> scheduledFuture = executorService.scheduleAtFixedRate(
                this::crawl, 0, 1, TimeUnit.SECONDS);

        try {
            scheduledFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        log.info("Finished crawl job {}", uuid);
    }

    private void crawl() {
        if (!crawlFrontier.isEmpty()) {
            HttpUrl url = crawlFrontier.remove();

            Request getRequest = new Request.Builder()
                    .url(url.toString())
                    .build();

            try (Response response = httpClient.newCall(getRequest).execute()) {
                var documentLinksPair = harvestResponse(url, response);
                visited.add(url);
                crawlFrontier.addAll(documentLinksPair.getValue());

                CompletableFuture.runAsync(() -> {
                    try {
                        persistResponseBody(url, documentLinksPair.getKey());
                    } catch (IOException e) {
                        log.warn("Could not persist response file for url {}", url);
                    }
                });
            } catch (IOException e) {
                log.warn("Could not download page {}", url.toString(), e);
            }
        }
    }

    private Map.Entry<Document, Set<HttpUrl>> harvestResponse(HttpUrl url, Response response) throws IOException {
        log.info("Parsing response from {}", url);
        visited.add(url);

        ResponseBody responseBody = response.body();

        Document htmlDocument = Jsoup.parse(responseBody.byteStream(), "UTF-8", url.toString());

        Set<HttpUrl> urls = htmlDocument.select("a[href]").stream()
                .map(anchor -> anchor.attr("abs:href"))
                .map(HttpUrl::parse)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        log.trace("Found the following well formed absolute links: {}", urls);
        return new AbstractMap.SimpleEntry<>(htmlDocument, urls);
    }

    private void persistResponseBody(HttpUrl url, Document document) throws IOException {
        // todo: needs more work for directory and file naming
        Path hostPath = fileSystemTargetPath.resolve(url.scheme())
                .resolve(url.host());

        Path resourcePath = hostPath;
        for (String segment : url.pathSegments()) {
            if (!segment.equals("")) {
                resourcePath = resourcePath.resolve(segment);
            } else {
                resourcePath = resourcePath.resolve("index.html");
            }
        }

        Files.createDirectories(resourcePath.getParent());
        Files.writeString(resourcePath, document.toString());
    }
}
