package me.mircea.bdt.crawl.config;

import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CrawlerConfiguration {
    @Bean
    public OkHttpClient httpClient() {
        return new OkHttpClient();
    }
}
