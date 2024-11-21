package cache.dataimportes.holders;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author Manjunath Kustagi
 */
public class TranscriptMappingResults implements Comparable<TranscriptMappingResults>, Serializable {

    /**
     * Column definition
     */
    public static final String[] headerNames = {"Experiment ID", "Internal ID", "Transcript ID", "Transcript URL"};

    public enum QUERY_TYPE {GENE, TRANSCRIPT};
    
    public long transcriptID;
    public String name;
    public String symbol;
    public String biotype;
    public long mappedCount;
    public long totalMappedCount;
    public String mappedAlignments;
    public List<Integer> mappedAlignmentCounts;
    public double rpkm;
    public int transcriptLength;
    public Map<Integer, List<Long>> multiMappers;
    public Map<Integer, List<Long>> rcMultiMappers;
    public List<Integer> multiMapperCount;
    public List<Integer> rcMultiMapperCount;
    public Map<String, List<Integer>> coverages;

    public String geneSymbol = "-";
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
    
    @Override
    public int compareTo(TranscriptMappingResults t) {
        return new Long(mappedCount).compareTo(t.mappedCount);
    }    
}
