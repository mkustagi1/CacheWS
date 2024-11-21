package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Experimentsummary class corresponding to <code>experimentsummary</code> cassandra table
 */
@Table(keyspace = "cache", name = "experimentsummary",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Experimentsummary implements Serializable {

    @PartitionKey(0)
    @Column(name = "experimentid")
    private long experimentid;

    @ClusteringColumn(0)
    @Column(name = "distance")
    private int distance;

    @ClusteringColumn(1)
    @Column(name = "transcriptid")
    private long transcriptid;

    @ClusteringColumn(2)
    @Column(name = "symbol")
    private String symbol;

    @ClusteringColumn(3)
    @Column(name = "id")
    private UUID id;

    @Column(name = "filtered")
    private boolean filtered;

    @Column(name = "mappedreadsbylength")
    private double mappedreadsbylength;

    @Column(name = "nonredundantreads")
    private long nonredundantreads;
    
    @Column(name = "rpkm")
    private double rpkm;

    @Column(name = "totalreads")
    private long totalreads;

    @Column(name = "transcriptlength")
    private long transcriptlength;
    
    @Column(name = "transcriptsmappedbyreads")
    private long transcriptsmappedbyreads;

    public Experimentsummary() {
    }

    public Experimentsummary(long experimentid, long transcriptid, String symbol, 
            UUID id, boolean filtered, double mappedreadsbylength, long nonredundantreads, 
            double rpkm, long totalreads, long transcriptlength, long transcriptsmappedbyreads) {
        this.experimentid = experimentid;
        this.transcriptid = transcriptid;
        this.symbol = symbol;
        this.id = id;
        this.filtered = filtered;
        this.mappedreadsbylength = mappedreadsbylength;
        this.nonredundantreads = nonredundantreads;
        this.rpkm = rpkm;
        this.totalreads = totalreads;
        this.transcriptlength = transcriptlength;
        this.transcriptsmappedbyreads = transcriptsmappedbyreads;
    }

    public long getExperimentid() {
        return this.experimentid;
    }

    public void setExperimentid(long experimentid) {
        this.experimentid = experimentid;
    }

    public int getDistance() {
        return this.distance;
    }

    public void setDistance(int d) {
        this.distance = d;
    }        
    
    public long getTranscriptid() {
        return this.transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }    
    
    public String getSymbol() {
        return this.symbol;
    }

    public void setSymbol(String s) {
        this.symbol = s;
    }

    public UUID getId() {
        return this.id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public boolean getFiltered() {
        return this.filtered;
    }

    public void setFiltered(boolean f) {
        this.filtered = f;
    }

    public double getMappedreadsbylength() {
        return this.mappedreadsbylength;
    }

    public void setMappedreadsbylength(double mr) {
        this.mappedreadsbylength = mr;
    }

    public long getNonredundantreads() {
        return this.nonredundantreads;
    }

    public void setNonredundantreads(long nr) {
        this.nonredundantreads = nr;
    }

    public double getRpkm() {
        return this.rpkm;
    }

    public void setRpkm(double r) {
        this.rpkm = r;
    }

    public long getTotalreads() {
        return this.totalreads;
    }

    public void setTotalreads(long tr) {
        this.totalreads = tr;
    }

    public long getTranscriptlength() {
        return this.transcriptlength;
    }

    public void setTranscriptlength(long tl) {
        this.transcriptlength = tl;
    }

    public long getTranscriptsmappedbyreads() {
        return this.transcriptsmappedbyreads;
    }

    public void setTranscriptsmappedbyreads(long tm) {
        this.transcriptsmappedbyreads = tm;
    }
    
    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Experimentsummary)) {
            return false;
        }
        Experimentsummary castOther = (Experimentsummary) other;

        return (this.getExperimentid() == castOther.getExperimentid())
                && (this.getTranscriptid() == castOther.getTranscriptid())
                && (this.getSymbol().equals(castOther.getSymbol()));
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (int) this.getTranscriptid();
        result = 37 * result + (int) this.getSymbol().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return this.getExperimentid() + ", " + this.getDistance() + ", " + this.getSymbol() + ", " + this.getRpkm();
    }
}
