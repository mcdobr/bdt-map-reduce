package me.mircea.bdt.crawl;

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Queue;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class SequentialCrawler {
    private final Queue<HttpUrl> crawlFrontier;
    private final Set<HttpUrl> visited;
    private final OkHttpClient httpClient;

//    @Value("${crawler.seeds}")
//    private ImmutableList<String> seeds;
    @Value("${crawler.page.limit}")
    private long pageLimit;
//    @Value("${crawler.filesystem.target.path}")
//    private Path fileSystemTargetPath;

    public void crawl() {
        while (visited.size() < pageLimit) {
            crawlOnePage();
        }
    }

    private void crawlOnePage() {
        while (!crawlFrontier.isEmpty()) {
            HttpUrl url = crawlFrontier.remove();

            Request getRequest = new Request.Builder()
                    .url(url.toString())
                    .build();

            try (Response response = httpClient.newCall(getRequest).execute()) {
                ResponseBody responseBody = response.body();

                Document htmlDocument = Jsoup.parse(responseBody.byteStream(), "UTF-8", url.toString());

                htmlDocument.select("a[href]").stream()
                        .map(anchor -> anchor.attr("abs:href"))
                        .map(HttpUrl::get)
                        .forEach(visited::add);
            } catch (IOException e) {
                log.info("Could not download page {}", url.toString(), e);
            }
        }
    }

}
