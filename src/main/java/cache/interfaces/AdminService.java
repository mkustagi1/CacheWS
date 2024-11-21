package cache.interfaces;

import cache.dataimportes.holders.DataStatisticsResults;
import cache.dataimportes.holders.SearchResult;
import cache.dataimportes.holders.Strand;
import cache.dataimportes.holders.SummaryResults;
import java.util.List;

/**
 *
 * @author Manjunath Kustagi
 */
public interface AdminService {

    public enum Status {GREEN, YELLOW, RED};
    
    public void indexDatabase();

    public void indexDatabase_20();

    public void indexDatabase_40();

    public void indexDatabase_48();

    public void mergeIndexes(long expId, int threadCount);

    public void mergeIndexes_20(long expId, int threadCount);

    public void mergeIndexes_40(long expId, int threadCount);

    public void mergeIndexes_200(long expId, int threadCount);

    public void searchDatabases();

    public void indexDatabaseReads();

    public void indexDatabaseReadsForExperiment(long experimentId);

    public void indexDatabaseReadsForExperiment_10(long experimentId);

    public void indexDatabaseReadsForExperiment_20(long experimentId);

    public void indexDatabaseReadsForExperiment_40(long experimentId);

    public void indexDatabaseReadsForExperiment_200(long experimentId);

    public List<SearchResult> searchReads(int key, long experimentId, int distance, String query, int maxCount, int searchLength, SearchResult.SearchType searchType, Strand strand);

    public List<SearchResult> searchDatabase(String query, int key, int distance, int maxCount, int searchLength, SearchResult.SearchType searchType, Strand strand);

    public List<SummaryResults> getSummary(int key, long experimentId, int distance);

    public List<SummaryResults> getSummariesForGenes(String geneSymbol, List<String> types);

    public void persistBiotypesFromSummary(SummaryResults sr, String user, String authToken);
    
    public void editBiotypes(String search, String replace, String user, String authToken);
    
    public void editSummary(SummaryResults sr, boolean allExperiments, String user, String authToken);
    
    public void updateSummary(int key, long experimentId, int distance);

    public DataStatisticsResults getDataStatistics(long experimentId, int distance);

    public void composeOverlappingReadRegions(long experimentId);

    public boolean createUser(String user, String authToken, String email);
    
    public boolean authenticateUser(String user, String authToken);
    
    public Status getServerStatus();
}
