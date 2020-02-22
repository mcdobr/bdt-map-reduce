package me.mircea.bdt.crawl.config;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedList;
import java.util.Queue;

@Configuration
public class CrawlerConfiguration {

    @Bean
    public OkHttpClient httpClient() {
        return new OkHttpClient();
    }

    @Bean
    public Queue<HttpUrl> crawlFrontier() {
        return new LinkedList<>();
    }

//    @Bean
//    public
}
