package me.mircea.bdt.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@Builder
public class ResourceDownload {
    private URI uri;
    private Instant downloadedTimestamp;
    private Path path;
}
