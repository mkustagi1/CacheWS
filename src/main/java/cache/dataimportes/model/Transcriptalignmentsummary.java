package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Transcriptalignmentsummary class corresponding to <code>transcriptalignmentsummary</code> cassandra table
 */
@Table(keyspace = "cache", name = "transcriptalignmentsummary",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Transcriptalignmentsummary implements Serializable {

    @PartitionKey(0)
    @Column(name = "experimentid")
    private long experimentid;

    @ClusteringColumn(0)
    @Column(name = "distance")
    private int distance;

    @ClusteringColumn(1)
    @Column(name = "transcriptid")
    private long transcriptid;

    @Column(name = "alignment")
    private ByteBuffer alignment;

    @Column(name = "coverages")
    private ByteBuffer coverages;

    @Column(name = "mappedreads")
    private long mappedreads;

    @Column(name = "multimappers")
    private ByteBuffer multimappers;
    
    @Column(name = "totalmappedreads")
    private long totalmappedreads;

    public Transcriptalignmentsummary() {
    }

    public Transcriptalignmentsummary(long experimentid, long transcriptid, int distance, 
            ByteBuffer alignment, ByteBuffer coverages, long mappedreads, ByteBuffer multimappers, long totalmappedreads) {
        this.experimentid = experimentid;
        this.transcriptid = transcriptid;
        this.distance = distance;
        this.alignment = alignment;
        this.coverages = coverages;
        this.mappedreads = mappedreads;
        this.multimappers = multimappers;
        this.totalmappedreads = totalmappedreads;
    }

    public long getExperimentid() {
        return this.experimentid;
    }

    public void setExperimentid(long experimentid) {
        this.experimentid = experimentid;
    }

    public long getTranscriptid() {
        return this.transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }    
    
    public int getDistance() {
        return this.distance;
    }

    public void setDistance(int d) {
        this.distance = d;
    }

    public ByteBuffer getAlignment() {
        return this.alignment;
    }

    public void setAlignment(ByteBuffer a) {
        this.alignment = a;
    }

    public ByteBuffer getCoverages() {
        return this.coverages;
    }

    public void setCoverages(ByteBuffer c) {
        this.coverages = c;
    }

    public long getMappedreads() {
        return this.mappedreads;
    }

    public void setMappedreads(long mappedreads) {
        this.mappedreads = mappedreads;
    }

    public ByteBuffer getMultimappers() {
        return this.multimappers;
    }

    public void setMultimappers(ByteBuffer multimappers) {
        this.multimappers = multimappers;
    }

    public long getTotalmappedreads() {
        return this.totalmappedreads;
    }

    public void setTotalmappedreads(long totalmappedreads) {
        this.totalmappedreads = totalmappedreads;
    }

    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Transcriptalignmentsummary)) {
            return false;
        }
        Transcriptalignmentsummary castOther = (Transcriptalignmentsummary) other;

        return (this.getExperimentid() == castOther.getExperimentid())
                && (this.getTranscriptid() == castOther.getTranscriptid())
                && (this.getDistance() == castOther.getDistance());
    }

    @Override
    public int hashCode() {
        int result = 17;

        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (int) this.getTranscriptid();
        result = 37 * result + (int) this.getDistance();
        return result;
    }

}
