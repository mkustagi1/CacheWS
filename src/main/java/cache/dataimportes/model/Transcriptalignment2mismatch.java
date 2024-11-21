package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Transcriptalignment1mismatch class corresponding to <code>transcriptalignment2mismatch</code> cassandra table
 */
@Table(keyspace = "cache", name = "transcriptalignment2mismatch",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Transcriptalignment2mismatch implements Serializable {

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
    @Column(name = "mismatch1type")
    private String mismatch1type;

    @ClusteringColumn(3)
    @Column(name = "mismatch2type")
    private String mismatch2type;

    @ClusteringColumn(4)
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

    @Column(name = "mismatch1coordinate")
    private Integer mismatch1coordinate;

    @Column(name = "variant1")
    private String variant1;
    
    @Column(name = "mismatch2coordinate")
    private Integer mismatch2coordinate;

    @Column(name = "variant2")
    private String variant2;

    public Transcriptalignment2mismatch() {
    }

    public Transcriptalignment2mismatch(UUID id) {
        this.id = id;
    }

    public Transcriptalignment2mismatch(long experimentid, long transcriptid, UUID id, 
            UUID readid, boolean reversecomplement, long startcoordinate, 
            long stopcoordinate, Long readCount, double score, Integer mismatch1coordinate, String mismatch1type, String variant1,
            Integer mismatch2coordinate, String mismatch2type, String variant2) {
        this.experimentid = experimentid;
        this.transcriptid = transcriptid;
        this.id = id;
        this.readid = readid;
        this.reversecomplement = reversecomplement;
        this.startcoordinate = startcoordinate;
        this.stopcoordinate = stopcoordinate;
        this.readcount = readCount;
        this.score = score;
        this.mismatch1coordinate = mismatch1coordinate;
        this.mismatch1type = mismatch1type;
        this.variant1 = variant1;
        this.mismatch2coordinate = mismatch2coordinate;
        this.mismatch2type = mismatch2type;
        this.variant2 = variant2;
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

    public Integer getMismatch1coordinate() {
        return this.mismatch1coordinate;
    }

    public void setMismatch1coordinate(Integer mismatch1coordinate) {
        this.mismatch1coordinate = mismatch1coordinate;
    }

    public String getMismatch1type() {
        return this.mismatch1type;
    }

    public void setMismatch1type(String mismatch1type) {
        this.mismatch1type = mismatch1type;
    }

    public String getVariant1() {
        return this.variant1;
    }

    public void setVariant1(String variant1) {
        this.variant1 = variant1;
    }

    public Integer getMismatch2coordinate() {
        return this.mismatch2coordinate;
    }

    public void setMismatch2coordinate(Integer mismatch2coordinate) {
        this.mismatch2coordinate = mismatch2coordinate;
    }

    public String getMismatch2type() {
        return this.mismatch2type;
    }

    public void setMismatch2type(String mismatch2type) {
        this.mismatch2type = mismatch2type;
    }

    public String getVariant2() {
        return this.variant2;
    }

    public void setVariant2(String variant2) {
        this.variant2 = variant2;
    }

    @Override
    public String toString() {
        return mismatch1type + ", " + variant1 + ", " + mismatch1coordinate + 
                ", " + mismatch2type + ", " + variant2 + ", " + mismatch2coordinate;
    }
    
    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Transcriptalignment2mismatch)) {
            return false;
        }
        Transcriptalignment2mismatch castOther = (Transcriptalignment2mismatch) other;

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
