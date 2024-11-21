package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Transcriptannotations class corresponding to <code>transcriptannotations</code> cassandra table
 */
@Table(keyspace = "cache", name = "transcriptannotations",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Transcriptannotations implements Serializable {

    @PartitionKey(0)
    @Column(name = "key")
    private int key;

    @ClusteringColumn(0)
    @Column(name = "transcriptid")
    private long transcriptid;

    @ClusteringColumn(1)
    @Column(name = "annotation")
    private String annotation;

    @ClusteringColumn(2)
    @Column(name = "startcoordinate")
    private Long startcoordinate;

    @ClusteringColumn(3)
    @Column(name = "stopcoordinate")
    private Long stopcoordinate;

    @ClusteringColumn(4)
    @Column(name = "id")
    private UUID id;

    @Column(name = "predicted")
    private Boolean predicted;

    @Column(name = "reversecomplement")
    private Boolean reversecomplement;

    @Column(name = "sequence")
    private String sequence;

    @Column(name = "variant")
    private String variant;

    public Transcriptannotations() {
    }

    public Transcriptannotations(int key, long transcriptid, String annotation, Long startcoordinate, Long stopcoordinate, UUID id, Boolean predicted, Boolean reversecomplement, String sequence, String variant) {
        this.key = key;
        this.transcriptid = transcriptid;
        this.annotation = annotation;
        this.startcoordinate = startcoordinate;
        this.stopcoordinate = stopcoordinate;
        this.id = id;
        this.predicted = predicted;
        this.reversecomplement = reversecomplement;
        this.sequence = sequence;
        this.variant = variant;
    }
    
    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public long getTranscriptid() {
        return transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    public String getAnnotation() {
        return annotation;
    }

    public void setAnnotation(String annotation) {
        this.annotation = annotation;
    }

    public Long getStartcoordinate() {
        return startcoordinate;
    }

    public void setStartcoordinate(Long startcoordinate) {
        this.startcoordinate = startcoordinate;
    }

    public Long getStopcoordinate() {
        return stopcoordinate;
    }

    public void setStopcoordinate(Long stopcoordinate) {
        this.stopcoordinate = stopcoordinate;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Boolean getPredicted() {
        return predicted;
    }

    public void setPredicted(Boolean predicted) {
        this.predicted = predicted;
    }

    public Boolean getReversecomplement() {
        return reversecomplement;
    }

    public void setReversecomplement(Boolean reversecomplement) {
        this.reversecomplement = reversecomplement;
    }

    public String getSequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getVariant() {
        return variant;
    }

    public void setVariant(String variant) {
        this.variant = variant;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + this.key;
        hash = 23 * hash + (int) (this.transcriptid ^ (this.transcriptid >>> 32));
        hash = 23 * hash + Objects.hashCode(this.annotation);
        hash = 23 * hash + Objects.hashCode(this.startcoordinate);
        hash = 23 * hash + Objects.hashCode(this.stopcoordinate);
        hash = 23 * hash + Objects.hashCode(this.id);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Transcriptannotations other = (Transcriptannotations) obj;
        if (this.key != other.key) {
            return false;
        }
        if (this.transcriptid != other.transcriptid) {
            return false;
        }
        if (this.annotation != other.annotation) {
            return false;
        }
        if (!Objects.equals(this.startcoordinate, other.startcoordinate)) {
            return false;
        }
        if (!Objects.equals(this.stopcoordinate, other.stopcoordinate)) {
            return false;
        }
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        return true;
    }

    
}
