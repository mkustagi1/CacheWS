package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Objects;

/**
 * Transcript class corresponding to <code>transcript</code> cassandra table
 */
@Table(keyspace = "cache", name = "transcript",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Transcript implements Serializable {

    @PartitionKey
    @Column(name = "key")
    private Integer key;

    @ClusteringColumn(0)
    @Column(name = "transcriptid")
    private long transcriptid;

    @ClusteringColumn(1)
    @Column(name = "name")
    private String name;

    @Column(name = "sequence")
    private String sequence;

    public Transcript() {
    }

    public Transcript(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    public Transcript(Integer key, long transcriptid, String name, String sequence) {
        this.key = key;
        this.transcriptid = transcriptid;
        this.name = name;
        this.sequence = sequence;
    }

    public Integer getKey() {
        return this.key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public long getTranscriptid() {
        return this.transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    public String getName() {
        return this.name;
    }

    public void setname(String name) {
        this.name = name;
    }

    public String getSequence() {
        return this.sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Transcript)) {
            return false;
        }
        Transcript castOther = (Transcript) other;

        return (this.getName().equals(castOther.getName())
                && (Objects.equals(this.getKey(), castOther.getKey()))
                && (this.getTranscriptid() == castOther.getTranscriptid()));
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getKey();
        result = 37 * result + (int) this.getTranscriptid();
        result = 37 * result + (int) this.getName().hashCode();
        result = 37 * result + (int) this.getSequence().hashCode();
        return result;
    }

}
