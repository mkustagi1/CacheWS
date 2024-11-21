package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;

/**
 * Biotypes class corresponding to <code>biotypes</code> cassandra table
 */
@Table(keyspace = "cache", name = "biotypes",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Biotypes implements Serializable {

    @PartitionKey(0)
    @Column(name = "key")
    private int key;

    @ClusteringColumn(0)
    @Column(name = "transcriptid")
    private long transcriptid;

    @Column(name = "biotype")
    private String biotype;

    @Column(name = "disease")
    private String disease;

    @Column(name = "hierarchy0isoform")
    private String hierarchy0isoform;

    @Column(name = "hierarchy0mutation")
    private String hierarchy0mutation;

    @Column(name = "hierarchy0other")
    private String hierarchy0other;
    
    @Column(name = "hierarchydown")
    private String hierarchydown;

    @Column(name = "hierarchyup")
    private String hierarchyup;

    @Column(name = "leveliclassification")
    private String leveliclassification;
    
    @Column(name = "levelifunction")
    private String levelifunction;

    @Column(name = "leveligenestructure")
    private String leveligenestructure;

    @Column(name = "leveliiclassification")
    private String leveliiclassification;

    @Column(name = "leveliiiclassification")
    private String leveliiiclassification;

    @Column(name = "levelivclassification")
    private String levelivclassification;

    @Column(name = "roleincancers")
    private String roleincancers;

    @Column(name = "symbol")
    private String symbol;

    @Column(name = "synonymousnames")
    private String synonymousnames;
    
    
    public Biotypes() {
    }

    public Biotypes(int key, long transcriptid, String biotype, String disease, String hierarchy0isoform, String hierarchy0mutation, String hierarchy0other, String hierarchydown, String hierarchyup, String leveliclassification, String levelifunction, String leveligenestructure, String leveliiclassification, String leveliiiclassification, String levelivclassification, String roleincancers, String symbol, String synonymousnames) {
        this.key = key;
        this.transcriptid = transcriptid;
        this.biotype = biotype;
        this.disease = disease;
        this.hierarchy0isoform = hierarchy0isoform;
        this.hierarchy0mutation = hierarchy0mutation;
        this.hierarchy0other = hierarchy0other;
        this.hierarchydown = hierarchydown;
        this.hierarchyup = hierarchyup;
        this.leveliclassification = leveliclassification;
        this.levelifunction = levelifunction;
        this.leveligenestructure = leveligenestructure;
        this.leveliiclassification = leveliiclassification;
        this.leveliiiclassification = leveliiiclassification;
        this.levelivclassification = levelivclassification;
        this.roleincancers = roleincancers;
        this.symbol = symbol;
        this.synonymousnames = synonymousnames;
    }    
    
    /**
     * @return the key
     */
    public int getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(int key) {
        this.key = key;
    }

    /**
     * @return the transcriptid
     */
    public long getTranscriptid() {
        return transcriptid;
    }

    /**
     * @param transcriptid the transcriptid to set
     */
    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    /**
     * @return the biotype
     */
    public String getBiotype() {
        return biotype;
    }

    /**
     * @param biotype the biotype to set
     */
    public void setBiotype(String biotype) {
        this.biotype = biotype;
    }

    /**
     * @return the disease
     */
    public String getDisease() {
        return disease;
    }

    /**
     * @param disease the disease to set
     */
    public void setDisease(String disease) {
        this.disease = disease;
    }

    /**
     * @return the hierarchy0isoform
     */
    public String getHierarchy0isoform() {
        return hierarchy0isoform;
    }

    /**
     * @param hierarchy0isoform the hierarchy0isoform to set
     */
    public void setHierarchy0isoform(String hierarchy0isoform) {
        this.hierarchy0isoform = hierarchy0isoform;
    }

    /**
     * @return the hierarchy0mutation
     */
    public String getHierarchy0mutation() {
        return hierarchy0mutation;
    }

    /**
     * @param hierarchy0mutation the hierarchy0mutation to set
     */
    public void setHierarchy0mutation(String hierarchy0mutation) {
        this.hierarchy0mutation = hierarchy0mutation;
    }

    /**
     * @return the hierarchy0other
     */
    public String getHierarchy0other() {
        return hierarchy0other;
    }

    /**
     * @param hierarchy0other the hierarchy0other to set
     */
    public void setHierarchy0other(String hierarchy0other) {
        this.hierarchy0other = hierarchy0other;
    }

    /**
     * @return the hierarchydown
     */
    public String getHierarchydown() {
        return hierarchydown;
    }

    /**
     * @param hierarchydown the hierarchydown to set
     */
    public void setHierarchydown(String hierarchydown) {
        this.hierarchydown = hierarchydown;
    }

    /**
     * @return the hierarchyup
     */
    public String getHierarchyup() {
        return hierarchyup;
    }

    /**
     * @param hierarchyup the hierarchyup to set
     */
    public void setHierarchyup(String hierarchyup) {
        this.hierarchyup = hierarchyup;
    }

    /**
     * @return the leveliclassification
     */
    public String getLeveliclassification() {
        return leveliclassification;
    }

    /**
     * @param leveliclassification the leveliclassification to set
     */
    public void setLeveliclassification(String leveliclassification) {
        this.leveliclassification = leveliclassification;
    }

    /**
     * @return the levelifunction
     */
    public String getLevelifunction() {
        return levelifunction;
    }

    /**
     * @param levelifunction the levelifunction to set
     */
    public void setLevelifunction(String levelifunction) {
        this.levelifunction = levelifunction;
    }

    /**
     * @return the leveligenestructure
     */
    public String getLeveligenestructure() {
        return leveligenestructure;
    }

    /**
     * @param leveligenestructure the leveligenestructure to set
     */
    public void setLeveligenestructure(String leveligenestructure) {
        this.leveligenestructure = leveligenestructure;
    }

    /**
     * @return the leveliiclassification
     */
    public String getLeveliiclassification() {
        return leveliiclassification;
    }

    /**
     * @param leveliiclassification the leveliiclassification to set
     */
    public void setLeveliiclassification(String leveliiclassification) {
        this.leveliiclassification = leveliiclassification;
    }

    /**
     * @return the leveliiiclassification
     */
    public String getLeveliiiclassification() {
        return leveliiiclassification;
    }

    /**
     * @param leveliiiclassification the leveliiiclassification to set
     */
    public void setLeveliiiclassification(String leveliiiclassification) {
        this.leveliiiclassification = leveliiiclassification;
    }

    /**
     * @return the levelivclassification
     */
    public String getLevelivclassification() {
        return levelivclassification;
    }

    /**
     * @param levelivclassification the levelivclassification to set
     */
    public void setLevelivclassification(String levelivclassification) {
        this.levelivclassification = levelivclassification;
    }

    /**
     * @return the roleincancers
     */
    public String getRoleincancers() {
        return roleincancers;
    }

    /**
     * @param roleincancers the roleincancers to set
     */
    public void setRoleincancers(String roleincancers) {
        this.roleincancers = roleincancers;
    }

    /**
     * @return the symbol
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * @param symbol the symbol to set
     */
    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    /**
     * @return the synonymousnames
     */
    public String getSynonymousnames() {
        return synonymousnames;
    }

    /**
     * @param synonymousnames the synonymousnames to set
     */
    public void setSynonymousnames(String synonymousnames) {
        this.synonymousnames = synonymousnames;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + this.key;
        hash = 89 * hash + (int) (this.transcriptid ^ (this.transcriptid >>> 32));
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
        final Biotypes other = (Biotypes) obj;
        if (this.key != other.key) {
            return false;
        }
        if (this.transcriptid != other.transcriptid) {
            return false;
        }
        return true;
    }
}
