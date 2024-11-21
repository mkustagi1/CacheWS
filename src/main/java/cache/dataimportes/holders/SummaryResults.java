package cache.dataimportes.holders;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Manjunath Kustagi
 */
public class SummaryResults implements Comparable<SummaryResults>, Serializable{
    
    /**
     * Table Data
     */
    public String geneId;
    public long masterReadsNonRedundant;
    public long masterReadsTotal;
    public long mappedTranscriptsByMasterReads;
    public long masterTranscriptId;
    public String masterTranscript;
    public long masterTranscriptLength;
    public int isoforms;
    public long otherReadsCount;
    public double mappedReadByLength;
    public double rpkm;
    public int rpkmRank;
    public double rpkm1;
    public int rpkmRank1;
    public double rpkmDiff;
    public boolean filtered;
    
    public List<Integer> otherRanks = new ArrayList<>();
    public List<Double> otherRpkms = new ArrayList<>();
    public double rankVariance;
    public double rankEntropy;
    public double rpkmVariance;
    public double rpkmEntropy;
    public long experimentId = 0;
    public Integer distance = 0;
    public String experimentName;
    
    public String geneSymbol = "-";
    public String biotype = "-";
    public String synonymousNames = "-";
    public String levelIclassification = "-";
    public String levelIIclassification = "-";
    public String levelIIIclassification = "-";
    public String levelIVclassification = "-";
    public String hierarchyup = "-";
    public String hierarchydown = "-";
    public String hierarchy0isoform = "-";
    public String hierarchy0mutation = "-";
    public String hierarchy0other = "-";
    public String levelIfunction = "-";
    public String levelIgenestructure = "-";
    public String disease = "-";
    public String roleincancers = "-";    
    public List<Integer> annotations = new ArrayList<>();
    
    @Override
    public int compareTo(SummaryResults t) {
        return (new Double(rpkm)).compareTo(t.rpkm);
    }    

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.geneId);
        return hash;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof SummaryResults) {
            return ((SummaryResults)o).geneId.equals(geneId);
        }
        return false;
    }
}
