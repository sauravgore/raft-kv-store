package dev.example.raftkv.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "commerce")
public class CommerceProperties {
    private long idempotencyRetentionHours = 24 * 7;

    public long getIdempotencyRetentionHours() {
        return idempotencyRetentionHours;
    }

    public void setIdempotencyRetentionHours(long value) {
        this.idempotencyRetentionHours = value;
    }
}
