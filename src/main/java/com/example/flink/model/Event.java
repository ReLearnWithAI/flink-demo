package com.example.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;

/**
 * Event data model for Flink Kafka streaming
 */
public class Event implements Serializable {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("data")
    private String data;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("processed_at")
    private Long processedAt;
    
    @JsonProperty("enriched")
    private Boolean enriched;
    
    public Event() {
        this.timestamp = Instant.now().toEpochMilli();
    }
    
    public Event(String id, String type, String data, String source) {
        this();
        this.id = id;
        this.type = type;
        this.data = data;
        this.source = source;
    }
    
    // Getters and Setters
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getData() {
        return data;
    }
    
    public void setData(String data) {
        this.data = data;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public Long getProcessedAt() {
        return processedAt;
    }
    
    public void setProcessedAt(Long processedAt) {
        this.processedAt = processedAt;
    }
    
    public Boolean getEnriched() {
        return enriched;
    }
    
    public void setEnriched(Boolean enriched) {
        this.enriched = enriched;
    }
    
    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                ", timestamp=" + timestamp +
                ", source='" + source + '\'' +
                ", processedAt=" + processedAt +
                ", enriched=" + enriched +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        Event event = (Event) o;
        
        return id != null ? id.equals(event.id) : event.id == null;
    }
    
    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
} 