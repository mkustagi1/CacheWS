package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Experiment class corresponding to <code>experiment</code> cassandra table
 */
@Table(keyspace = "cache", name = "experiment",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Experiment implements Serializable {

    @PartitionKey(0)
    @Column(name = "key")
    private int key;

    @PartitionKey(1)
    @Column(name = "id")
    private long id;

    @PartitionKey(2)
    @Column(name = "distance")
    private int distance;

    @ClusteringColumn(0)
    @Column(name = "library")
    private String library;

    @ClusteringColumn(1)
    @Column(name = "investigator")
    private String investigator;

    @ClusteringColumn(2)
    @Column(name = "annotation")
    private String annotation;

    @ClusteringColumn(3)
    @Column(name = "method")
    private String method;

    @ClusteringColumn(4)
    @Column(name = "readlength")
    private String readlength;

    @Column(name = "attributes")
    private Map<String, String> attributes;

    public Experiment() {
    }

    public Experiment(int key, long id, int distance, String library, 
            String investigator, String annotation, String method, 
            String readlength, Map<String, String> attributes) {
        this.key = key;
        this.id = id;
        this.distance = distance;
        this.library = library;
        this.investigator = investigator;
        this.annotation = annotation;
        this.method = method;
        this.readlength = readlength;
        this.attributes = attributes;
    }

    public int getKey() {
        return this.key;
    }

    public void setKey(int k) {
        this.key = k;
    }            
    
    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getDistance() {
        return this.distance;
    }

    public void setDistance(int d) {
        this.distance = d;
    }        
    
    public String getLibrary() {
        return this.library;
    }

    public void setLibrary(String l) {
        this.library = l;
    }
    
    public String getInvestigator() {
        return this.investigator;
    }

    public void setInvestigator(String i) {
        this.investigator = i;
    }

    public String getAnnotation() {
        return this.annotation;
    }

    public void setAnnotation(String a) {
        this.annotation = a;
    }

    public String getMethod() {
        return this.method;
    }

    public void setMethod(String m) {
        this.method = m;
    }

    public String getReadlength() {
        return this.readlength;
    }

    public void setReadlength(String rl) {
        this.readlength = rl;
    }
    
    public Map<String, String> getAttributes() {
        return this.attributes;
    }
    
    public void setAttributes(Map<String, String> at) {
        this.attributes = at;
    }
    
    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Experiment)) {
            return false;
        }
        Experiment castOther = (Experiment) other;

        return (this.getId() == castOther.getId())
                && (this.getDistance() == castOther.getDistance())
                && (this.getLibrary().equals(castOther.getLibrary()));
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getId();
        result = 37 * result + (int) this.getDistance();
        result = 37 * result + (int) this.getLibrary().hashCode();
        return result;
    }

}
