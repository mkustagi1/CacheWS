package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Objects;

/**
 * Method class corresponding to <code>method</code> cassandra table
 */
@Table(keyspace = "cache", name = "method",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Method implements Serializable {

    @PartitionKey(0)
    @Column(name = "key")
    private int key;

    @PartitionKey(1)
    @Column(name = "method")
    private String method;

    @PartitionKey(2)
    @Column(name = "directionality")
    private Boolean directionality;

    @PartitionKey(3)
    @Column(name = "experimentid")
    private long experimentid;

    public Method() {
    }

    public Method(int k, String l, boolean d, long t) {
        this.key = k;
        this.method = l;
        directionality = d;
        this.experimentid = t;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Boolean getDirectionality() {
        return directionality;
    }

    public void setDirectionality(Boolean d) {
        this.directionality = d;
    }

    public long getExperimentid() {
        return experimentid;
    }

    public void setExperimentid(long experimentid) {
        this.experimentid = experimentid;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 13 * hash + this.key;
        hash = 13 * hash + Objects.hashCode(this.method);
        hash = 13 * hash + Objects.hashCode(this.directionality);
        hash = 13 * hash + (int) (this.experimentid ^ (this.experimentid >>> 32));
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
        final Method other = (Method) obj;
        if (this.key != other.key) {
            return false;
        }

        if (!Objects.equals(this.directionality, other.directionality)) {
            return false;
        }

        if (this.experimentid != other.experimentid) {
            return false;
        }
        return Objects.equals(this.method, other.method);
    }
}