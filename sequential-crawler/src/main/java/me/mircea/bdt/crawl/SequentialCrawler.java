package me.mircea.bdt.crawl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
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
    private final Path fileSystemDocumentsPath;

    @NonNull
    private final Path fileSystemBackLinksPath;

    @NonNull
    private final Path fileSystemReverseLookupPath;

    @NonNull
    private final Path fileSystemConnectivityPath;

    @NonNull
    private final UUID uuid = UUID.randomUUID();

    private final long pageLimit;

    private final Map<HttpUrl, List<String>> referrerMap = new HashMap<>();

    public void start() {
        log.info("Started crawl job {} with crawl frontier: {}", uuid, crawlFrontier);

        crawl();

        log.info("Finished crawl job {}", uuid);
    }

    private void crawl() {
        while (!crawlFrontier.isEmpty() && visited.size() < pageLimit) {
            HttpUrl url = crawlFrontier.remove();
            visited.add(url);

            Request getRequest = new Request.Builder()
                    .url(url.toString())
                    .build();

            try (Response response = httpClient.newCall(getRequest).execute()) {
                var documentLinksPair = harvestResponse(url, response);
                crawlFrontier.addAll(documentLinksPair.getValue());
                referrerMap.put(url, documentLinksPair.getValue().stream().map(HttpUrl::toString).collect(Collectors.toList()));

                CompletableFuture.runAsync(() -> tryToPersistResponseBody(url, documentLinksPair));
                CompletableFuture.runAsync(() -> {
                    documentLinksPair.getValue()
                            .forEach(referred -> tryToCreateBackLinkFiles(url, referred, Hashing.sipHash24()));
                });

                Thread.sleep(1000);
            } catch (IOException e) {
                log.warn("Could not download page {}", url.toString(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        Path jsonFile = fileSystemConnectivityPath.resolve("graph.json");
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Files.createFile(jsonFile);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile.toFile(), referrerMap);
        } catch (IOException e) {
            log.error("Could not write connectivity graph to disk");
        }
    }

    private Map.Entry<Document, Set<HttpUrl>> harvestResponse(HttpUrl url, @NonNull Response response) throws IOException {
        log.info("Parsing response from {}", url);
        visited.add(url); // todo: check existence of url hash on file system

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

    private void tryToPersistResponseBody(HttpUrl url, Map.Entry<Document, Set<HttpUrl>> documentLinksPair) {
        try {
            persistResponseBody(url, documentLinksPair.getKey());
        } catch (IOException e) {
            log.warn("Could not persist response file for url {}", url);
        }
    }

    private void persistResponseBody(HttpUrl url, Document document) throws IOException {
        Path resourcePath = fileSystemDocumentsPath.resolve(url.scheme())
                .resolve(url.host());
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

    private void tryToCreateBackLinkFiles(HttpUrl referrer, HttpUrl referred, HashFunction hashFunction) {
        HashCode referredHash = hashUrl(referred, hashFunction);
        HashCode referrerHash = hashUrl(referrer, hashFunction);

        String backLinkFileName = String.format("%s_%s.backLink",
                referredHash.toString(),
                referrerHash.toString()
        );
        String reverseLookupFileName = generateRandomString();

        try {
            Path backLinkPath = Files.createFile(fileSystemBackLinksPath.resolve(backLinkFileName));
            log.debug("Created file {}", backLinkPath);

            Path reversePath = Files.createFile(fileSystemReverseLookupPath.resolve(reverseLookupFileName));
            Files.writeString(reversePath, referredHash.toString() + " " + referred);
        } catch (FileAlreadyExistsException e) {
            log.debug("Back link {} already exists", backLinkFileName);
        } catch (IOException e) {
            log.warn("Could not create file with name {}", backLinkFileName);
        }
    }

    private HashCode hashUrl(HttpUrl url, HashFunction hashFunction) {
        return hashFunction.newHasher()
                .putString(url.toString(), Charsets.UTF_8)
                .hash();
    }

    private String generateRandomString() {
        final int bufferSize = 64;
        final byte[] buffer = new byte[bufferSize];
        ThreadLocalRandom.current().nextBytes(buffer);
        return BaseEncoding.base32().omitPadding().encode(buffer);
    }
}

// have some reverse mapping files that you can merge later
// and just use the hashes to put on the files

