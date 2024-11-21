package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Transcriptalignment2mismatchtranspose class corresponding to <code>transcriptalignment2mismatchtranspose</code> cassandra table
 */
@Table(keyspace = "cache", name = "transcriptalignment2mismatchtranspose",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Transcriptalignment2mismatchtranspose implements Serializable {

    @PartitionKey(0)
    @Column(name = "experimentid")
    private long experimentid;

    @ClusteringColumn(0)
    @Column(name = "readid")
    private UUID readid;

    @ClusteringColumn(1)
    @Column(name = "transcriptid")
    private long transcriptid;

    @Column(name = "count")
    private int count;    
    
    public Transcriptalignment2mismatchtranspose() {
    }

    public Transcriptalignment2mismatchtranspose(long experimentid, long transcriptid, UUID readid, Integer count) {
        this.experimentid = experimentid;
        this.transcriptid = transcriptid;
        this.readid = readid;
        this.count = count;
    }

    public long getExperimentid() {
        return this.experimentid;
    }

    public void setExperimentid(long experimentid) {
        this.experimentid = experimentid;
    }

    public UUID getReadid() {
        return this.readid;
    }

    public void setReadid(UUID readid) {
        this.readid = readid;
    }

    public long getTranscriptid() {
        return this.transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    public int getCount() {
        return this.count;
    }

    public void setCount(int count) {
        this.count = count;
    }    

    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Transcriptalignmenttranspose)) {
            return false;
        }
        Transcriptalignmenttranspose castOther = (Transcriptalignmenttranspose) other;

        return (this.getExperimentid() == castOther.getExperimentid())
                && ((this.getReadid() == null ? castOther.getReadid() == null : this.getReadid().equals(castOther.getReadid())) || (this.getReadid() != null && castOther.getReadid() != null && this.getReadid().equals(castOther.getReadid())))
                && (this.getTranscriptid() == castOther.getTranscriptid());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (getReadid() == null ? 0 : this.getReadid().hashCode());
        result = 37 * result + (int) this.getTranscriptid();
        return result;
    }

}
