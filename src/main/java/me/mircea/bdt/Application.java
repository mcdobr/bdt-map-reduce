package me.mircea.bdt;

import me.mircea.bdt.crawl.SequentialCrawler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    private SequentialCrawler sequentialCrawler;

    @Override
    public void run(String... args) throws Exception {
        sequentialCrawler.crawl();
    }
}
