package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Transcriptalignment1mismatch class corresponding to <code>transcriptalignment1mismatch</code> cassandra table
 */
@Table(keyspace = "cache", name = "transcriptalignment1mismatch",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Transcriptalignment1mismatch implements Serializable {

    @PartitionKey(0)
    @Column(name = "experimentid")
    private long experimentid;

    @ClusteringColumn(0)
    @Column(name = "transcriptid")
    private long transcriptid;

    @ClusteringColumn(1)
    @Column(name = "reversecomplement")
    private boolean reversecomplement;

    @ClusteringColumn(2)
    @Column(name = "mismatchtype")
    private String mismatchtype;

    @ClusteringColumn(3)
    @Column(name = "id")
    private UUID id;

    @Column(name = "readid")
    private UUID readid;

    @Column(name = "startcoordinate")
    private long startcoordinate;

    @Column(name = "score")
    private Double score;

    @Column(name = "stopcoordinate")
    private Long stopcoordinate;

    @Column(name = "readcount")
    private Long readcount;

    @Column(name = "mismatchcoordinate")
    private Integer mismatchcoordinate;

    @Column(name = "variant")
    private String variant;
    
    public Transcriptalignment1mismatch() {
    }

    public Transcriptalignment1mismatch(UUID id) {
        this.id = id;
    }

    public Transcriptalignment1mismatch(long experimentid, long transcriptid, UUID id, 
            UUID readid, boolean reversecomplement, long startcoordinate, 
            long stopcoordinate, Long readCount, double score, Integer mismatchcoordinate, String mismatchtype, String variant) {
        this.experimentid = experimentid;
        this.transcriptid = transcriptid;
        this.id = id;
        this.readid = readid;
        this.reversecomplement = reversecomplement;
        this.startcoordinate = startcoordinate;
        this.stopcoordinate = stopcoordinate;
        this.readcount = readCount;
        this.score = score;
        this.mismatchcoordinate = mismatchcoordinate;
        this.mismatchtype = mismatchtype;
        this.variant = variant;
    }

    public long getExperimentid() {
        return this.experimentid;
    }

    public void setExperimentid(long experimentid) {
        this.experimentid = experimentid;
    }

    public UUID getId() {
        return this.id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getReadid() {
        return this.readid;
    }

    public void setReadid(UUID readid) {
        this.readid = readid;
    }

    public boolean getReversecomplement() {
        return this.reversecomplement;
    }

    public void setReversecomplement(boolean reversecomplement) {
        this.reversecomplement = reversecomplement;
    }

    public long getStartcoordinate() {
        return this.startcoordinate;
    }

    public void setStartcoordinate(long startcoordinate) {
        this.startcoordinate = startcoordinate;
    }

    public long getReadcount() {
        return this.readcount;
    }

    public void setReadcount(long readcount) {
        this.readcount = readcount;
    }

    public long getTranscriptid() {
        return this.transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    public Double getScore() {
        return this.score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Long getStopcoordinate() {
        return this.stopcoordinate;
    }

    public void setStopcoordinate(Long stopcoordinate) {
        this.stopcoordinate = stopcoordinate;
    }

    public Integer getMismatchcoordinate() {
        return this.mismatchcoordinate;
    }

    public void setMismatchcoordinate(Integer mismatchcoordinate) {
        this.mismatchcoordinate = mismatchcoordinate;
    }

    public String getMismatchtype() {
        return this.mismatchtype;
    }

    public void setMismatchtype(String mismatchtype) {
        this.mismatchtype = mismatchtype;
    }

    public String getVariant() {
        return this.variant;
    }

    public void setVariant(String variant) {
        this.variant = variant;
    }

    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Transcriptalignment1mismatch)) {
            return false;
        }
        Transcriptalignment1mismatch castOther = (Transcriptalignment1mismatch) other;

        return (this.getExperimentid() == castOther.getExperimentid())
                && ((this.getId() == null ? castOther.getId() == null : this.getId().equals(castOther.getId())) || (this.getId() != null && castOther.getId() != null && this.getId().equals(castOther.getId())))
                && ((this.getReadid() == null ? castOther.getReadid() == null : this.getReadid().equals(castOther.getReadid())) || (this.getReadid() != null && castOther.getReadid() != null && this.getReadid().equals(castOther.getReadid())))
                && (this.getReversecomplement() == castOther.getReversecomplement())
                && (this.getStartcoordinate() == castOther.getStartcoordinate())
                && (this.getTranscriptid() == castOther.getTranscriptid());
    }

    @Override
    public int hashCode() {
        int result = 17;

        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (getId() == null ? 0 : this.getId().hashCode());
        result = 37 * result + (getReadid() == null ? 0 : this.getReadid().hashCode());
        result = 37 * result + (this.getReversecomplement() ? 1 : 0);
        result = 37 * result + (int) this.getStartcoordinate();
        result = 37 * result + (int) this.getTranscriptid();
        return result;
    }

}
