package cache;

import cache.dataimportes.CreateExperimentSummaries;
import cache.dataimportes.holders.DataStatisticsResults;
import cache.dataimportes.holders.SearchResult;
import cache.dataimportes.holders.Strand;
import cache.dataimportes.holders.SummaryResults;
import cache.dataimportes.model.Biotypes;
import cache.dataimportes.model.Datastatistics;
import cache.dataimportes.model.Experiment;
import cache.dataimportes.model.Experimentkey;
import cache.dataimportes.model.Experimentsummary;
import cache.dataimportes.model.Levelclassification;
import cache.dataimportes.model.Method;
import cache.dataimportes.model.Transcriptannotations;
import cache.dataimportes.model.Users;
import static cache.dataimportes.util.DataAccess.BIOTYPESMAPPER;
import static cache.dataimportes.util.DataAccess.BIOTYPESQUERY;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSMAPPER;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTKEYMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTREADLENGTHQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTSUMMARYMAPPER;
import static cache.dataimportes.util.DataAccess.LEVELCLASSIFICATIONMAPPER;
import static cache.dataimportes.util.DataAccess.LEVELCLASSIFICATIONQUERY;
import static cache.dataimportes.util.DataAccess.METHODMAPPER;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTANNOTATIONSMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTQUERY;
import static cache.dataimportes.util.DataAccess.USERSMAPPER;
import static cache.dataimportes.util.DataAccess.USERSQUERY;
import cache.dataimportes.util.DataAccessES;
import static cache.dataimportes.util.DataAccessES.SYNONYMSEARCHBUILDER;
import static cache.dataimportes.util.DataAccessES.TRANSCRIPTSEARCHBUILDER;
import static cache.dataimportes.util.DataAccessES.editTranscriptSynonyms;
import cache.dataimportes.util.Fuzzy;
import cache.interfaces.AdminService;
import com.caucho.hessian.server.HessianServlet;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.views.ComplementSequenceView;
import org.biojava.nbio.core.sequence.views.ReversedSequenceView;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 *
 * @author Manjunath Kustagi
 */
public class AdminServiceImpl extends HessianServlet implements AdminService {

    @Override
    public void indexDatabase() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabase_20() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabase_40() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabase_48() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void mergeIndexes(long expId, int threadCount) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void mergeIndexes_20(long expId, int threadCount) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void mergeIndexes_40(long expId, int threadCount) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void mergeIndexes_200(long expId, int threadCount) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void searchDatabases() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabaseReads() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabaseReadsForExperiment(long experimentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabaseReadsForExperiment_10(long experimentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabaseReadsForExperiment_20(long experimentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabaseReadsForExperiment_40(long experimentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void indexDatabaseReadsForExperiment_200(long experimentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<SearchResult> searchReads(int key, long experimentId, int distance, String query, int maxCount, int searchLength, SearchResult.SearchType searchType, Strand strand) {
        final List<SearchResult> searchResults = new ArrayList<>();
        List<SearchResult> filtered = new ArrayList<>();
        try {
            BoundStatement bs = EXPERIMENTREADLENGTHQUERY.bind().setInt("ky", key).setLong("id", experimentId).setInt("d", distance);
            ResultSet _rs = SESSION.execute(bs);
            int primaryReadLength = Integer.parseInt(_rs.one().getString("readLength"));

            if (searchLength == primaryReadLength) {
                switch (distance) {
                    case 0:
                        searchResults.addAll(DataAccessES.searchReadsExactMatch(experimentId, query, maxCount, searchLength, true, searchType, strand));
                        break;
                    case 1:
                        searchResults.addAll(DataAccessES.searchReads1Mismatch(experimentId, query, maxCount, searchLength, searchType, strand));
                        break;
                    case 2:
                        searchResults.addAll(DataAccessES.searchReads2Mismatches(experimentId, query, maxCount, searchLength, searchType, strand));
                        break;
                    default:
                        searchResults.addAll(DataAccessES.searchReadsExactMatch(experimentId, query, maxCount, searchLength, true, searchType, strand));
                        break;
                }
            } else if ((searchLength < primaryReadLength) && (searchLength > 40)) {

                searchResults.addAll(DataAccessES.searchReadsExactMatch40ScrollingWithMaxCount(experimentId, maxCount, query, SearchResult.SearchType.READS, strand));

                for (SearchResult sr : searchResults) {
                    if (sr.strand.equals(Strand.FORWARD)) {
                        List<String> fTokens = new ArrayList<>();
                        int index = 0;
                        while (index <= (sr.sequence.length() - searchLength)) {
                            fTokens.add(sr.sequence.substring(index, index + searchLength));
                            index++;
                        }
                        Fuzzy fuzzy = new Fuzzy();
                        if (!fuzzy.containsOneOfExactMatch(query, fTokens)) {
                            filtered.add(sr);
                        }
                    } else {
                        List<String> rcTokens = new ArrayList<>();
                        int index = 0;
                        DNASequence sequence = new DNASequence(sr.sequence);
                        org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                                = new ReversedSequenceView<>(
                                        new ComplementSequenceView<>(sequence));
                        String revComp = rc.getSequenceAsString();
                        while (index <= (revComp.length() - searchLength)) {
                            rcTokens.add(revComp.substring(index, index + searchLength));
                            index++;
                        }
                        Fuzzy fuzzy = new Fuzzy();
                        if (!fuzzy.containsOneOfExactMatch(query, rcTokens)) {
                            filtered.add(sr);
                        }
                    }
                }

                filtered.stream().forEach((f) -> {
                    searchResults.remove(f);
                });
            } else {
                searchResults.addAll(DataAccessES.searchReadsExactMatch40ScrollingWithMaxCount(experimentId, maxCount, query, SearchResult.SearchType.READS, strand));
            }
        } catch (NumberFormatException | CompoundNotFoundException ex) {
            Logger.getLogger(AdminServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }

        return searchResults;
    }

    @Override
    public List<SearchResult> searchDatabase(String query, int key, int distance, int maxCount, int searchLength, SearchResult.SearchType searchType, Strand strand) {
        List<SearchResult> searchResults;

        if (searchLength >= 101) {
            searchLength = 101;
        } else if (searchLength >= 97) {
            searchLength = 97;
        } else if (searchLength >= 75 && searchLength < 97) {
            searchLength = 75;
        } else if (searchLength >= 65 && searchLength < 75) {
            searchLength = 65;
        } else if (searchLength >= 50 && searchLength < 65) {
            searchLength = 50;
        } else if (searchLength >= 40 && searchLength < 50) {
            searchLength = 40;
        } else {
            searchLength = 101;
        }

        switch (distance) {
            case 0:
                searchResults = DataAccessES.searchTranscriptsExactMatch(query, maxCount, searchLength, searchType, strand);
                break;
            case 1:
                searchResults = DataAccessES.searchTranscripts1Mismatch(query, maxCount, searchLength, searchType, strand);
                break;
            case 2:
                searchResults = DataAccessES.searchTranscripts2Mismatches(query, maxCount, searchLength, searchType, strand);
                break;
            default:
                searchResults = DataAccessES.searchTranscriptsExactMatch(query, maxCount, searchLength, searchType, strand);
                break;
        }

        return searchResults;
    }

    @Override
    public List<SummaryResults> getSummary(int key, long experimentId, int distance) {
        List<SummaryResults> sresults = new ArrayList<>();
        try {

            key = EXPERIMENTKEYMAPPER.get(experimentId, distance).getKey();
            BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", key).setLong("id", experimentId).setInt("d", distance);
            ResultSet _rs = SESSION.execute(bs);
            Experiment experiment = EXPERIMENTMAPPER.map(_rs).one();

            bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", distance);
            _rs = SESSION.execute(bs);
            Datastatistics ds = DATASTATISTICSMAPPER.map(_rs).one();

            Strand strand;
            if (experiment.getAnnotation().contains("directional")) {
                if (experiment.getAnnotation().endsWith("_FORWARD")) {
                    strand = Strand.FORWARD;
                } else {
                    strand = Strand.REVERSECOMPLEMENT;
                }
            } else {
                strand = Strand.BOTH;
            }

            Statement stmt2 = QueryBuilder
                    .select()
                    .all()
                    .from("anvesana", "experimentsummary")
                    .where(eq("experimentid", experimentId))
                    .and(eq("distance", distance));
            ResultSet _rs1 = SESSION.execute(stmt2);
            List<Experimentsummary> results = EXPERIMENTSUMMARYMAPPER.map(_rs1).all();
            int count = 0;
            for (Experimentsummary result : results) {
                SummaryResults sr = new SummaryResults();
                sr.geneId = Long.toString(result.getTranscriptid());
                sr.geneSymbol = result.getSymbol();
                Biotypes bt = BIOTYPESMAPPER.get(1, result.getTranscriptid());
                String biotype = "";
                boolean specialCategory = false;
                if (bt == null) {
                    biotype = "Unavailable";
                } else {
                    biotype = (bt.getBiotype() == null) ? "" : bt.getBiotype();
                    sr.biotype = biotype;
                    sr.masterTranscriptId = bt.getTranscriptid();
                    sr.geneSymbol = bt.getSymbol();
                    sr.synonymousNames = bt.getSynonymousnames();
                    sr.levelIclassification = bt.getLeveliclassification();
                    sr.levelIIclassification = bt.getLeveliiclassification();
                    sr.levelIIIclassification = bt.getLeveliiiclassification();
                    sr.levelIVclassification = bt.getLevelivclassification();
                    sr.hierarchyup = bt.getHierarchyup();
                    sr.hierarchydown = bt.getHierarchydown();
                    sr.hierarchy0isoform = bt.getHierarchy0isoform();
                    sr.hierarchy0mutation = bt.getHierarchy0mutation();
                    sr.hierarchy0other = bt.getHierarchy0other();
                    sr.levelIfunction = bt.getLevelifunction();
                    sr.levelIgenestructure = bt.getLeveligenestructure();
                    sr.disease = bt.getDisease();
                    sr.roleincancers = bt.getRoleincancers();

                    if (sr.levelIIIclassification.equalsIgnoreCase("immunoglobulin chr 14")
                            //                                || sr.levelIIIclassification.equalsIgnoreCase("immunoglobulin chr 22")
                            || sr.levelIclassification.equalsIgnoreCase("human mitochondrial")
                            || sr.levelIclassification.equalsIgnoreCase("human LINE")
                            || sr.levelIIclassification.equalsIgnoreCase("miRNA")
                            || sr.levelIIclassification.equalsIgnoreCase("pri_miRNA")
                            || sr.levelIIclassification.equalsIgnoreCase("pre_miRNA")
                            || sr.levelIIclassification.equalsIgnoreCase("miRNA_minor")
                            //|| sr.levelIIclassification.equalsIgnoreCase("lncRNA")
                            //                                || sr.levelIIclassification.equalsIgnoreCase("scaRNA")
                            //                                || sr.levelIIclassification.equalsIgnoreCase("scRNA")
                            //                                || sr.levelIIclassification.equalsIgnoreCase("snoRNA")
                            //                                || sr.levelIIclassification.equalsIgnoreCase("snRNA")
                            || sr.levelIIclassification.equalsIgnoreCase("rRNA")
                            //                                || sr.levelIIclassification.equalsIgnoreCase("tRNA")
                            || sr.levelIclassification.equalsIgnoreCase("calibrator")
                            //                                || sr.levelIIclassification.equalsIgnoreCase("pre_tRNA")
                            || sr.levelIIclassification.equalsIgnoreCase("virus_adenovirus")
                            || sr.levelIclassification.equalsIgnoreCase("bacterium_mycoplasma")
                            || sr.levelIclassification.equalsIgnoreCase("Illumina DNA")
                            || sr.levelIclassification.equalsIgnoreCase("RNA size marker")
                            || sr.levelIclassification.equalsIgnoreCase("sRNA cDNA lib adapter")
                            || sr.levelIclassification.equalsIgnoreCase("recombinant DNA")
                            || sr.levelIclassification.equalsIgnoreCase("virus_EBV")) {
                        specialCategory = true;
                    }

                }

                if (specialCategory) {
                    continue;
                }

                sr.biotype = biotype;
                stmt2 = QueryBuilder
                        .select()
                        .all()
                        .from("anvesana", "transcriptannotations")
                        .where(eq("key", 1))
                        .and(eq("transcriptid", result.getTranscriptid()));
                _rs1 = SESSION.execute(stmt2);

                List<Transcriptannotations> transcriptAnnotations = TRANSCRIPTANNOTATIONSMAPPER.map(_rs1).all();

                Integer sc = 0;
                Integer stc = 0;
                Integer ss = 0;
                Integer polya = 0;
                Integer cs = 0;
                Integer ff = 0;
                Integer variants = 0;
                Integer sirna = 0;
                Integer other = 0;
                Integer promoter = 0;
                Integer antiSense = 0;
                Integer sscoordinate = 0;
                Integer acs = 0;
                Integer afsse = 0;
                Integer atsse = 0;
                Integer fse = 0;
                Integer lse = 0;
                Integer mee = 0;
                Integer ritu = 0;
                Integer ri = 0;
                Integer ins = 0;
                Integer del = 0;
                Integer tra = 0;
                List<Integer> promoterCoordinate = new ArrayList<>();
                Integer scCoordinate = 0;
                for (Transcriptannotations ta : transcriptAnnotations) {
                    String annotation = ta.getAnnotation();
                    if (annotation.startsWith("siRNA")) {
                        sirna++;
                    } else if (annotation.equals("Start Codon")) {
                        sc++;
                        scCoordinate = ta.getStartcoordinate().intValue();
                    } else if (annotation.equals("Stop Codon")) {
                        stc++;
                    } else if (annotation.equals("Start Site")) {
                        ss++;
                        sscoordinate = ta.getStartcoordinate().intValue();
                    } else if (annotation.equals("Cleavage Site")) {
                        cs++;
                    } else if (annotation.equals("PolyA Signal")) {
                        polya++;
                    } else if (annotation.equals("Substitution")) {
                        variants++;
                    } else if (annotation.equals("Alternative Cassette Exon")) {
                        acs++;
                    } else if (annotation.equals("Alternative 5’ Splice Site Exon")) {
                        afsse++;
                    } else if (annotation.equals("Alternative 3’ Splice Site Exon")) {
                        atsse++;
                    } else if (annotation.equals("1st Shared Exon")) {
                        fse++;
                    } else if (annotation.equals("Last Shared Exon")) {
                        lse++;
                    } else if (annotation.equals("Mutually Exclusive Exon")) {
                        mee++;
                    } else if (annotation.equals("Retained Intron 3’ UTR")) {
                        ritu++;
                    } else if (annotation.equals("Retained Intron")) {
                        ri++;
                    } else if (annotation.equals("Insertion")) {
                        ins++;
                    } else if (annotation.equals("Deletion")) {
                        del++;
                    } else if (annotation.equals("Transposition")) {
                        tra++;
                    } else if (annotation.equals("EPD_NEW")) {
                        promoter++;
                        if (ta.getReversecomplement()) {
                            antiSense++;
                        } else {
                            promoterCoordinate.add(ta.getStopcoordinate().intValue() - 11);
                        }
                    } else {
                        ff++;
                    }
                }

                Integer ssDiff = Integer.MAX_VALUE;
                if (ss == 1 && promoter > 0) {
                    for (int i = 0; i < promoterCoordinate.size(); i++) {
                        Integer t = sscoordinate - promoterCoordinate.get(i);
                        if (Math.abs(ssDiff) > Math.abs(t)) {
                            ssDiff = t;
                        }
                    }
                } else {
                    ssDiff = Integer.MAX_VALUE;
                }

                Integer scDiff = Integer.MAX_VALUE;
                if (sc == 1 && promoter > 0) {
                    for (int i = 0; i < promoterCoordinate.size(); i++) {
                        Integer t = scCoordinate - promoterCoordinate.get(i);
                        if (Math.abs(scDiff) > Math.abs(t)) {
                            scDiff = t;
                        }
                    }
                } else {
                    scDiff = Integer.MAX_VALUE;
                }

                List<Integer> annotations = new ArrayList<>();
                annotations.add(sc);
                annotations.add(stc);
                annotations.add(ss);
                annotations.add(polya);
                annotations.add(cs);
                annotations.add(variants);
                annotations.add(sirna);
                annotations.add(acs);
                annotations.add(afsse);
                annotations.add(atsse);
                annotations.add(fse);
                annotations.add(lse);
                annotations.add(mee);
                annotations.add(ritu);
                annotations.add(ri);
                annotations.add(ins);
                annotations.add(del);
                annotations.add(tra);
                annotations.add(ff);
                annotations.add(other);
                annotations.add(promoter);
                annotations.add(antiSense);
                annotations.add(ssDiff);
                annotations.add(scDiff);

                sr.annotations = annotations;
                sr.isoforms = 1;
                sr.mappedReadByLength = result.getMappedreadsbylength();
                sr.mappedTranscriptsByMasterReads = result.getTranscriptsmappedbyreads();
                sr.masterReadsNonRedundant = result.getNonredundantreads();
                sr.masterReadsTotal = result.getTotalreads();
                sr.masterTranscript = result.getSymbol();
                sr.masterTranscriptId = result.getTranscriptid();
                sr.masterTranscriptLength = result.getTranscriptlength();
//                sr.otherReadsCount = result.
                sr.rpkm = result.getRpkm();
                sr.rpkmRank = count++;
                sresults.add(sr);
            }

            SummaryResults sr = collateSpecialCategory("immunoglobulin chr 14", "III", experimentId, distance, strand, ds);
            sresults.add(sr);
//            sr = collateSpecialCategory("immunoglobulin chr 22", "III", experimentId);
//            sresults.add(sr);
            sr = collateSpecialCategory("human mitochondrial", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("human LINE", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("miRNA", "II", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("pri_miRNA", "II", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("pre_miRNA", "II", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("miRNA_minor", "II", experimentId, distance, strand, ds);
            sresults.add(sr);
//            sr = collateSpecialCategory("lncRNA", "II", experimentId);
//            sresults.add(sr);
//            sr = collateSpecialCategory("scaRNA", "II", experimentId);
//            sresults.add(sr);
//            sr = collateSpecialCategory("scRNA", "II", experimentId);
//            sresults.add(sr);
//            sr = collateSpecialCategory("snoRNA", "II", experimentId);
//            sresults.add(sr);
//            sr = collateSpecialCategory("snRNA", "II", experimentId);
//            sresults.add(sr);
            sr = collateSpecialCategory("rRNA", "II", experimentId, distance, strand, ds);
//            sresults.add(sr);
//            sr = collateSpecialCategory("tRNA", "II", experimentId);
            sresults.add(sr);
            sr = collateSpecialCategory("calibrator", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
//            sr = collateSpecialCategory("pre_tRNA", "II", experimentId);
            sresults.add(sr);
            sr = collateSpecialCategory("virus_adenovirus", "II", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("bacterium_mycoplasma", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("Illumina DNA", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("RNA size marker", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("sRNA cDNA lib adapter", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("recombinant DNA", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
            sr = collateSpecialCategory("virus_EBV", "I", experimentId, distance, strand, ds);
            sresults.add(sr);
        } catch (Exception ex) {
            Logger.getLogger(AdminServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        Collections.sort(sresults, Collections.reverseOrder());
        for (int i = 0; i < sresults.size(); i++) {
            SummaryResults sr = sresults.get(i);
            sr.rpkmRank = i;
            sresults.set(i, sr);
        }
        return sresults;
    }

    private SummaryResults collateSpecialCategory(String category, String level, long experimentId, int distance, Strand strand, Datastatistics ds) {
        SummaryResults sr = new SummaryResults();
        try {
            sr.geneId = category;
            sr.geneSymbol = category;
            Statement stmt2 = QueryBuilder
                    .select()
                    .all()
                    .from("anvesana", "levelclassification")
                    .where(eq("category", category))
                    .and(eq("level", level));
            List<Levelclassification> lcs = LEVELCLASSIFICATIONMAPPER.map(SESSION.execute(stmt2)).all();

            List<Long> transcripts = new ArrayList<>();
            lcs.stream().forEach((lc) -> {
                transcripts.add(lc.getTranscriptid());
            });

            if (transcripts.size() > 0) {
                sr.biotype = category;
                sr.isoforms = transcripts.size();
                long[] counts = CreateExperimentSummaries.getMappedReadCountsinTranscripts(experimentId, distance, transcripts, strand);
                sr.masterReadsNonRedundant = counts[0];
                sr.masterReadsTotal = counts[1];
                sr.mappedTranscriptsByMasterReads = (sr.masterReadsTotal == 0) ? 0 : transcripts.size() / sr.masterReadsTotal;
                sr.masterTranscript = category;
                sr.masterTranscriptId = -1;
                int average = 0;
                average = transcripts.stream().map((tid) -> TRANSCRIPTQUERY.bind().setLong("ti", tid)).map((bs) -> TRANSCRIPTMAPPER.map(SESSION.execute(bs)).one().getSequence().length()).reduce(average, Integer::sum);
                sr.masterTranscriptLength = average / transcripts.size();
                sr.mappedReadByLength = 1000 * sr.masterReadsTotal / sr.masterTranscriptLength;
                sr.otherReadsCount = 0;
                Long mappedReads;
                switch (strand) {
                    case FORWARD:
                        mappedReads = ds.getTotalmappedreads() - ds.getTotalmappedreadsRC();
                        break;
                    case REVERSECOMPLEMENT:
                        mappedReads = ds.getTotalmappedreadsRC();
                        break;
                    default:
                        mappedReads = ds.getTotalmappedreads();
                        break;
                }
                sr.rpkm = sr.mappedReadByLength / (mappedReads / 1000000d);
                sr.rpkmRank = -1;
            }
        } catch (Exception ex) {
            Logger.getLogger(AdminServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return sr;
    }

    @Override
    public List<SummaryResults> getSummariesForGenes(String geneSymbol, List<String> types) {
        List<SummaryResults> sresults = new ArrayList<>();
        try {
            boolean _exactMatch = false;
            boolean _1Mismatch = false;
            boolean _2Mismatch = false;
            boolean _all = false;
            String directionality = "_Directionality-all";
            for (String type : types) {
                switch (type) {
                    case "_Exact_Match":
                        _exactMatch = true;
                        break;
                    case "_1_Mismatch":
                        _1Mismatch = true;
                        break;
                    case "_2_Mismatch":
                        _2Mismatch = true;
                        break;
                    case "_All_Distances":
                        _all = true;
                        break;
                    case "_Directionality-all":
                        directionality = "_Directionality-all";
                        break;
                    case "_Directional":
                        directionality = "_Directional";
                        break;
                    case "_Non-Directional":
                        directionality = "_Non-Directional";
                        break;
                    default:
                        break;
                }
            }
            List<String> expIds = new ArrayList<>();
            Map<String, Experiment> experiments = new HashMap<>();
            for (String type : types) {
                if (!type.startsWith("_")) {
                    if (type.equals("Other")) {
                        Statement stmt2 = QueryBuilder
                                .select()
                                .all()
                                .from("anvesana", "method")
                                .where(eq("key", 1));
                        List<Method> methods = METHODMAPPER.map(SESSION.execute(stmt2)).all();
                        Map<String, Set<Experiment>> meMap = new HashMap<>();
                        List<String> otherTypes = new ArrayList<>();
                        methods.stream().map((Method method) -> {
                            if (!method.getMethod().equals("PAR-CLIP")
                                    && !method.getMethod().equals("TotalZero101")
                                    && !method.getMethod().equals("Total101")
                                    && !method.getMethod().equals("PolyA101")
                                    && !method.getMethod().equals("RiboP50")
                                    && !method.getMethod().equals("SingleCell")
                                    && !method.getMethod().equals("Hydro19-24")) {
                                otherTypes.add(method.getMethod());
                            } else {
                            }
                            return method;
                        }).forEach((method) -> {
                            Experimentkey ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 0);
                            BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 0);
                            ResultSet _rs = SESSION.execute(bs);
                            Experiment e = EXPERIMENTMAPPER.map(_rs).one();

                            ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 1);
                            bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 1);
                            _rs = SESSION.execute(bs);
                            Experiment e1 = EXPERIMENTMAPPER.map(_rs).one();

                            ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 2);
                            bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 2);
                            _rs = SESSION.execute(bs);
                            Experiment e2 = EXPERIMENTMAPPER.map(_rs).one();

                            if (!meMap.containsKey(method.getMethod())) {
                                Set<Experiment> eids = new HashSet<>();
                                eids.add(e);
                                eids.add(e1);
                                eids.add(e2);
                                meMap.put(method.getMethod(), eids);
                            } else {
                                meMap.get(method.getMethod()).add(e);
                                meMap.get(method.getMethod()).add(e1);
                                meMap.get(method.getMethod()).add(e2);
                            }
                        });
                        for (String ot : otherTypes) {
                            Set<Experiment> exps = (Set<Experiment>) meMap.get(ot);
                            for (Experiment e : exps) {
                                int distance = e.getDistance();
                                if (_exactMatch && distance == 0) {
                                    expIds.add(e.getId() + "_" + distance);
                                    experiments.put(e.getId() + "_" + distance, e);
                                }
                                if (_1Mismatch && distance == 1) {
                                    expIds.add(e.getId() + "_" + distance);
                                    experiments.put(e.getId() + "_" + distance, e);
                                }
                                if (_2Mismatch && distance == 2) {
                                    expIds.add(e.getId() + "_" + distance);
                                    experiments.put(e.getId() + "_" + distance, e);
                                }
                                if (_all) {
                                    expIds.add(e.getId() + "_" + distance);
                                    experiments.put(e.getId() + "_" + distance, e);
                                }
                            }
                        }
                    } else if (directionality.equals("_Directionality-all")) {
                        Statement stmt2 = QueryBuilder
                                .select()
                                .all()
                                .from("anvesana", "method")
                                .where(eq("key", 1))
                                .and(eq("method", type));
                        List<Method> methods = METHODMAPPER.map(SESSION.execute(stmt2)).all();
                        for (Method method : methods) {

                            Experimentkey ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 0);
                            BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 0);
                            ResultSet _rs = SESSION.execute(bs);
                            Experiment e = EXPERIMENTMAPPER.map(_rs).one();

                            ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 1);
                            bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 1);
                            _rs = SESSION.execute(bs);
                            Experiment e1 = EXPERIMENTMAPPER.map(_rs).one();

                            ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 2);
                            bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 2);
                            _rs = SESSION.execute(bs);
                            Experiment e2 = EXPERIMENTMAPPER.map(_rs).one();

                            if (_exactMatch) {
                                expIds.add(e.getId() + "_" + 0);
                                experiments.put(e.getId() + "_" + 0, e);
                            }
                            if (_1Mismatch) {
                                expIds.add(e.getId() + "_" + 1);
                                experiments.put(e.getId() + "_" + 1, e1);
                            }
                            if (_2Mismatch) {
                                expIds.add(e.getId() + "_" + 2);
                                experiments.put(e.getId() + "_" + 2, e2);
                            }
                            if (_all) {
                                expIds.add(e.getId() + "_" + 0);
                                experiments.put(e.getId() + "_" + 0, e);
                                expIds.add(e.getId() + "_" + 1);
                                experiments.put(e.getId() + "_" + 1, e1);
                                expIds.add(e.getId() + "_" + 2);
                                experiments.put(e.getId() + "_" + 2, e2);
                            }
                        }
                    } else {
                        Boolean _directionality = directionality.equals("_Directional");
                        Statement stmt2 = QueryBuilder
                                .select()
                                .all()
                                .from("anvesana", "method")
                                .where(eq("key", 1))
                                .and(eq("method", type))
                                .and(eq("directionality", _directionality));
                        List<Method> methods = METHODMAPPER.map(SESSION.execute(stmt2)).all();
                        for (Method method : methods) {

                            Experimentkey ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 0);
                            BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 0);
                            ResultSet _rs = SESSION.execute(bs);
                            Experiment e = EXPERIMENTMAPPER.map(_rs).one();

                            ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 1);
                            bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 1);
                            _rs = SESSION.execute(bs);
                            Experiment e1 = EXPERIMENTMAPPER.map(_rs).one();

                            ekey = EXPERIMENTKEYMAPPER.get(method.getExperimentid(), 2);
                            bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", method.getExperimentid()).setInt("d", 2);
                            _rs = SESSION.execute(bs);
                            Experiment e2 = EXPERIMENTMAPPER.map(_rs).one();

                            if (_exactMatch) {
                                expIds.add(e.getId() + "_" + 0);
                                experiments.put(e.getId() + "_" + 0, e);
                            }
                            if (_1Mismatch) {
                                expIds.add(e.getId() + "_" + 1);
                                experiments.put(e.getId() + "_" + 1, e1);
                            }
                            if (_2Mismatch) {
                                expIds.add(e.getId() + "_" + 2);
                                experiments.put(e.getId() + "_" + 2, e2);
                            }
                            if (_all) {
                                expIds.add(e.getId() + "_" + 0);
                                experiments.put(e.getId() + "_" + 0, e);
                                expIds.add(e.getId() + "_" + 1);
                                experiments.put(e.getId() + "_" + 1, e1);
                                expIds.add(e.getId() + "_" + 2);
                                experiments.put(e.getId() + "_" + 2, e2);
                            }
                        }
                    }
                }
            }

            List<Long> eids = new ArrayList<>();
            Statement stmt2 = QueryBuilder
                    .select()
                    .column("id")
                    .from("anvesana", "experiment");
            ResultSet _rs = SESSION.execute(stmt2);

            while (!_rs.isExhausted()) {
                Long eid = _rs.one().getLong("id");
                if (!eids.contains(eid)) {
                    eids.add(eid);
                }
            }

            if (types.contains(
                    "sequence_search")) {
                String sequence = geneSymbol;

                Set<Long> tids = new HashSet<>();
                SearchResponse response;

                response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("sequence", sequence)).get();

                for (SearchHit hit : response.getHits().getHits()) {
                    String _n = hit.field("transcriptid").getValue().toString();
                    tids.add(Long.parseLong(_n));
                }
                for (Long tid : tids) {
                    stmt2 = QueryBuilder
                            .select()
                            .all()
                            .from("anvesana", "experimentsummary")
                            .where(in("experimentid", eids))
                            .and(in("distance", new Object[]{0, 1, 2}))
                            .and(eq("transcriptid", tid));
                    _rs = SESSION.execute(stmt2);
                    List<Experimentsummary> results = EXPERIMENTSUMMARYMAPPER.map(_rs).all();
                    int count = 0;
                    for (Experimentsummary result : results) {
                        String expId = result.getExperimentid() + "_" + result.getDistance();
                        if (expIds.contains(expId)) {
                            SummaryResults sr = new SummaryResults();
                            sr.experimentId = Long.parseLong(expId.split("_")[0]);
                            sr.distance = result.getDistance();
                            Experiment e = experiments.get(expId);
                            sr.experimentName = e.getLibrary();
                            sr.geneId = Long.toString(result.getTranscriptid());
                            sr.geneSymbol = result.getSymbol();

                            BoundStatement bs = BIOTYPESQUERY.bind().setLong("ti", result.getTranscriptid());
                            List<Biotypes> biotypes = BIOTYPESMAPPER.map(SESSION.execute(bs)).all();

                            String biotype = "";
                            if (biotypes == null || biotypes.isEmpty()) {
                                biotype = "Unavailable";
                            } else if (biotypes.size() == 1) {
                                Biotypes bt = (Biotypes) biotypes.get(0);
                                biotype = bt.getBiotype();
                                sr.biotype = biotype;
                                sr.masterTranscriptId = bt.getTranscriptid();
                                sr.geneSymbol = bt.getSymbol();
                                sr.synonymousNames = bt.getSynonymousnames();
                                sr.levelIclassification = bt.getLeveliclassification();
                                sr.levelIIclassification = bt.getLeveliiclassification();
                                sr.levelIIIclassification = bt.getLeveliiiclassification();
                                sr.levelIVclassification = bt.getLevelivclassification();
                                sr.hierarchyup = bt.getHierarchyup();
                                sr.hierarchydown = bt.getHierarchydown();
                                sr.hierarchy0isoform = bt.getHierarchy0isoform();
                                sr.hierarchy0mutation = bt.getHierarchy0mutation();
                                sr.hierarchy0other = bt.getHierarchy0other();
                                sr.levelIfunction = bt.getLevelifunction();
                                sr.levelIgenestructure = bt.getLeveligenestructure();
                                sr.disease = bt.getDisease();
                                sr.roleincancers = bt.getRoleincancers();
                            }

                            stmt2 = QueryBuilder
                                    .select()
                                    .all()
                                    .from("anvesana", "transcriptannotations")
                                    .where(eq("key", 1))
                                    .and(eq("transcriptid", result.getTranscriptid()));
                            _rs = SESSION.execute(stmt2);

                            List<Transcriptannotations> transcriptAnnotations = TRANSCRIPTANNOTATIONSMAPPER.map(_rs).all();

                            Integer sc = 0;
                            Integer stc = 0;
                            Integer ss = 0;
                            Integer polya = 0;
                            Integer cs = 0;
                            Integer ff = 0;
                            Integer variants = 0;
                            Integer sirna = 0;
                            Integer other = 0;
                            Integer promoter = 0;
                            Integer antiSense = 0;
                            Integer sscoordinate = 0;
                            Integer acs = 0;
                            Integer afsse = 0;
                            Integer atsse = 0;
                            Integer fse = 0;
                            Integer lse = 0;
                            Integer mee = 0;
                            Integer ritu = 0;
                            Integer ri = 0;
                            Integer ins = 0;
                            Integer del = 0;
                            Integer tra = 0;
                            List<Integer> promoterCoordinate = new ArrayList<>();
                            Integer scCoordinate = 0;
                            for (Transcriptannotations ta : transcriptAnnotations) {
                                String annotation = ta.getAnnotation();
                                if (annotation.startsWith("siRNA")) {
                                    sirna++;
                                } else if (annotation.equals("Start Codon")) {
                                    sc++;
                                    scCoordinate = ta.getStartcoordinate().intValue();
                                } else if (annotation.equals("Stop Codon")) {
                                    stc++;
                                } else if (annotation.equals("Start Site")) {
                                    ss++;
                                    sscoordinate = ta.getStartcoordinate().intValue();
                                } else if (annotation.equals("Cleavage Site")) {
                                    cs++;
                                } else if (annotation.equals("PolyA Signal")) {
                                    polya++;
                                } else if (annotation.equals("Substitution")) {
                                    variants++;
                                } else if (annotation.equals("Alternative Cassette Exon")) {
                                    acs++;
                                } else if (annotation.equals("Alternative 5’ Splice Site Exon")) {
                                    afsse++;
                                } else if (annotation.equals("Alternative 3’ Splice Site Exon")) {
                                    atsse++;
                                } else if (annotation.equals("1st Shared Exon")) {
                                    fse++;
                                } else if (annotation.equals("Last Shared Exon")) {
                                    lse++;
                                } else if (annotation.equals("Mutually Exclusive Exon")) {
                                    mee++;
                                } else if (annotation.equals("Retained Intron 3’ UTR")) {
                                    ritu++;
                                } else if (annotation.equals("Retained Intron")) {
                                    ri++;
                                } else if (annotation.equals("Insertion")) {
                                    ins++;
                                } else if (annotation.equals("Deletion")) {
                                    del++;
                                } else if (annotation.equals("Transposition")) {
                                    tra++;
                                } else if (annotation.equals("EPD_NEW")) {
                                    promoter++;
                                    if (ta.getReversecomplement()) {
                                        antiSense++;
                                    } else {
                                        promoterCoordinate.add(ta.getStopcoordinate().intValue() - 11);
                                    }
                                } else {
                                    ff++;
                                }
                            }

                            Integer ssDiff = Integer.MAX_VALUE;
                            if (ss == 1 && promoter > 0) {
                                for (int i = 0; i < promoterCoordinate.size(); i++) {
                                    Integer t = sscoordinate - promoterCoordinate.get(i);
                                    if (Math.abs(ssDiff) > Math.abs(t)) {
                                        ssDiff = t;
                                    }
                                }
                            } else {
                                ssDiff = Integer.MAX_VALUE;
                            }

                            Integer scDiff = Integer.MAX_VALUE;
                            if (sc == 1 && promoter > 0) {
                                for (int i = 0; i < promoterCoordinate.size(); i++) {
                                    Integer t = scCoordinate - promoterCoordinate.get(i);
                                    if (Math.abs(scDiff) > Math.abs(t)) {
                                        scDiff = t;
                                    }
                                }
                            } else {
                                scDiff = Integer.MAX_VALUE;
                            }

                            List<Integer> annotations = new ArrayList<>();
                            annotations.add(sc);
                            annotations.add(stc);
                            annotations.add(ss);
                            annotations.add(polya);
                            annotations.add(cs);
                            annotations.add(variants);
                            annotations.add(sirna);
                            annotations.add(acs);
                            annotations.add(afsse);
                            annotations.add(atsse);
                            annotations.add(fse);
                            annotations.add(lse);
                            annotations.add(mee);
                            annotations.add(ritu);
                            annotations.add(ri);
                            annotations.add(ins);
                            annotations.add(del);
                            annotations.add(tra);
                            annotations.add(ff);
                            annotations.add(other);
                            annotations.add(promoter);
                            annotations.add(antiSense);
                            annotations.add(ssDiff);
                            annotations.add(scDiff);

                            sr.annotations = annotations;
                            sr.isoforms = 1;
                            sr.mappedReadByLength = result.getMappedreadsbylength();
                            sr.mappedTranscriptsByMasterReads = result.getTranscriptsmappedbyreads();
                            sr.masterReadsNonRedundant = result.getNonredundantreads();
                            sr.masterReadsTotal = result.getTotalreads();
                            sr.masterTranscript = result.getSymbol();
                            sr.masterTranscriptId = result.getTranscriptid();
                            sr.masterTranscriptLength = result.getTranscriptlength();
                            sr.otherReadsCount = 0;
                            sr.rpkm = result.getRpkm();
                            sr.rpkmRank = count++;
                            sresults.add(sr);
                        }
                    }
                }
            } else {
                SearchResponse response;

                if (geneSymbol.contains("'")) {
                    geneSymbol = geneSymbol.replaceAll("'", "''");
                    geneSymbol = geneSymbol.substring(0, geneSymbol.indexOf("''"));
                }

                response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("name", geneSymbol.toLowerCase())).get();
                if (response.getHits().getTotalHits() == 0l) {
                    response = SYNONYMSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("synonym", geneSymbol.toLowerCase())).get();
                }

                for (SearchHit hit : response.getHits().getHits()) {
                    Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                    stmt2 = QueryBuilder
                            .select()
                            .all()
                            .from("anvesana", "experimentsummary")
                            .where(in("experimentid", eids))
                            .and(in("distance", new Object[]{0, 1, 2}))
                            .and(eq("transcriptid", tid));
                    _rs = SESSION.execute(stmt2);
                    List<Experimentsummary> results = EXPERIMENTSUMMARYMAPPER.map(_rs).all();

                    int count = 0;
                    for (Experimentsummary result : results) {
                        String expId = result.getExperimentid() + "_" + result.getDistance();
                        if (expIds.contains(expId)) {
                            SummaryResults sr = new SummaryResults();
                            sr.experimentId = Long.parseLong(expId.split("_")[0]);
                            sr.distance = result.getDistance();
                            Experiment e = experiments.get(expId);
                            sr.experimentName = e.getLibrary();
                            sr.geneId = Long.toString(result.getTranscriptid());
                            sr.geneSymbol = result.getSymbol();

                            BoundStatement bs = BIOTYPESQUERY.bind().setLong("ti", result.getTranscriptid());
                            List<Biotypes> biotypes = BIOTYPESMAPPER.map(SESSION.execute(bs)).all();

                            String biotype = "";
                            if (biotypes == null || biotypes.isEmpty()) {
                                biotype = "Unavailable";
                            } else if (biotypes.size() == 1) {
                                Biotypes bt = (Biotypes) biotypes.get(0);
                                biotype = (bt.getBiotype() == null) ? "" : bt.getBiotype();
                                sr.biotype = biotype;
                                sr.masterTranscriptId = bt.getTranscriptid();
                                sr.geneSymbol = bt.getSymbol();
                                sr.synonymousNames = bt.getSynonymousnames();
                                sr.levelIclassification = bt.getLeveliclassification();
                                sr.levelIIclassification = bt.getLeveliiclassification();
                                sr.levelIIIclassification = bt.getLeveliiclassification();
                                sr.levelIVclassification = bt.getLevelivclassification();
                                sr.hierarchyup = bt.getHierarchyup();
                                sr.hierarchydown = bt.getHierarchydown();
                                sr.hierarchy0isoform = bt.getHierarchy0isoform();
                                sr.hierarchy0mutation = bt.getHierarchy0mutation();
                                sr.hierarchy0other = bt.getHierarchy0other();
                                sr.levelIfunction = bt.getLevelifunction();
                                sr.levelIgenestructure = bt.getLeveligenestructure();
                                sr.disease = bt.getDisease();
                                sr.roleincancers = bt.getRoleincancers();
                            }

                            stmt2 = QueryBuilder
                                    .select()
                                    .all()
                                    .from("anvesana", "transcriptannotations")
                                    .where(eq("key", 1))
                                    .and(eq("transcriptid", result.getTranscriptid()));
                            _rs = SESSION.execute(stmt2);

                            List<Transcriptannotations> transcriptAnnotations = TRANSCRIPTANNOTATIONSMAPPER.map(_rs).all();

                            Integer sc = 0;
                            Integer stc = 0;
                            Integer ss = 0;
                            Integer polya = 0;
                            Integer cs = 0;
                            Integer ff = 0;
                            Integer variants = 0;
                            Integer sirna = 0;
                            Integer other = 0;
                            Integer promoter = 0;
                            Integer antiSense = 0;
                            Integer sscoordinate = 0;
                            Integer acs = 0;
                            Integer afsse = 0;
                            Integer atsse = 0;
                            Integer fse = 0;
                            Integer lse = 0;
                            Integer mee = 0;
                            Integer ritu = 0;
                            Integer ri = 0;
                            Integer ins = 0;
                            Integer del = 0;
                            Integer tra = 0;
                            List<Integer> promoterCoordinate = new ArrayList<>();
                            Integer scCoordinate = 0;
                            for (Transcriptannotations ta : transcriptAnnotations) {
                                String annotation = ta.getAnnotation();
                                if (annotation.startsWith("siRNA")) {
                                    sirna++;
                                } else if (annotation.equals("Start Codon")) {
                                    sc++;
                                    scCoordinate = ta.getStartcoordinate().intValue();
                                } else if (annotation.equals("Stop Codon")) {
                                    stc++;
                                } else if (annotation.equals("Start Site")) {
                                    ss++;
                                    sscoordinate = ta.getStartcoordinate().intValue();
                                } else if (annotation.equals("Cleavage Site")) {
                                    cs++;
                                } else if (annotation.equals("PolyA Signal")) {
                                    polya++;
                                } else if (annotation.equals("Substitution")) {
                                    variants++;
                                } else if (annotation.equals("Alternative Cassette Exon")) {
                                    acs++;
                                } else if (annotation.equals("Alternative 5’ Splice Site Exon")) {
                                    afsse++;
                                } else if (annotation.equals("Alternative 3’ Splice Site Exon")) {
                                    atsse++;
                                } else if (annotation.equals("1st Shared Exon")) {
                                    fse++;
                                } else if (annotation.equals("Last Shared Exon")) {
                                    lse++;
                                } else if (annotation.equals("Mutually Exclusive Exon")) {
                                    mee++;
                                } else if (annotation.equals("Retained Intron 3’ UTR")) {
                                    ritu++;
                                } else if (annotation.equals("Retained Intron")) {
                                    ri++;
                                } else if (annotation.equals("Insertion")) {
                                    ins++;
                                } else if (annotation.equals("Deletion")) {
                                    del++;
                                } else if (annotation.equals("Transposition")) {
                                    tra++;
                                } else if (annotation.equals("EPD_NEW")) {
                                    promoter++;
                                    if (ta.getReversecomplement()) {
                                        antiSense++;
                                    } else {
                                        promoterCoordinate.add(ta.getStopcoordinate().intValue() - 11);
                                    }
                                } else {
                                    ff++;
                                }
                            }

                            Integer ssDiff = Integer.MAX_VALUE;
                            if (ss == 1 && promoter > 0) {
                                for (int i = 0; i < promoterCoordinate.size(); i++) {
                                    Integer t = sscoordinate - promoterCoordinate.get(i);
                                    if (Math.abs(ssDiff) > Math.abs(t)) {
                                        ssDiff = t;
                                    }
                                }
                            } else {
                                ssDiff = Integer.MAX_VALUE;
                            }

                            Integer scDiff = Integer.MAX_VALUE;
                            if (sc == 1 && promoter > 0) {
                                for (int i = 0; i < promoterCoordinate.size(); i++) {
                                    Integer t = scCoordinate - promoterCoordinate.get(i);
                                    if (Math.abs(scDiff) > Math.abs(t)) {
                                        scDiff = t;
                                    }
                                }
                            } else {
                                scDiff = Integer.MAX_VALUE;
                            }

                            List<Integer> annotations = new ArrayList<>();
                            annotations.add(sc);
                            annotations.add(stc);
                            annotations.add(ss);
                            annotations.add(polya);
                            annotations.add(cs);
                            annotations.add(variants);
                            annotations.add(sirna);
                            annotations.add(acs);
                            annotations.add(afsse);
                            annotations.add(atsse);
                            annotations.add(fse);
                            annotations.add(lse);
                            annotations.add(mee);
                            annotations.add(ritu);
                            annotations.add(ri);
                            annotations.add(ins);
                            annotations.add(del);
                            annotations.add(tra);
                            annotations.add(ff);
                            annotations.add(other);
                            annotations.add(promoter);
                            annotations.add(antiSense);
                            annotations.add(ssDiff);
                            annotations.add(scDiff);

                            sr.annotations = annotations;
                            sr.isoforms = 1;
                            sr.mappedReadByLength = result.getMappedreadsbylength();
                            sr.mappedTranscriptsByMasterReads = result.getTranscriptsmappedbyreads();
                            sr.masterReadsNonRedundant = result.getNonredundantreads();
                            sr.masterReadsTotal = result.getTotalreads();
                            sr.masterTranscript = result.getSymbol();
                            sr.masterTranscriptId = result.getTranscriptid();
                            sr.masterTranscriptLength = result.getTranscriptlength();
                            sr.otherReadsCount = 0;
                            sr.rpkm = result.getRpkm();
                            sr.rpkmRank = count++;
                            sresults.add(sr);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(AdminServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return sresults;
    }

    @Override
    public void persistBiotypesFromSummary(SummaryResults sr, String user, String authToken) {
        if (authenticateUser(user, authToken)) {
            Biotypes bt = BIOTYPESMAPPER.get(1, sr.masterTranscriptId);
            if (bt == null) {
                bt = new Biotypes();
                bt.setKey(1);
            }

            bt.setBiotype(sr.biotype);
            bt.setTranscriptid(sr.masterTranscriptId);
            bt.setSymbol(sr.geneSymbol);
            bt.setSynonymousnames(sr.synonymousNames);
            editTranscriptSynonyms(sr.masterTranscriptId, sr.synonymousNames);
            bt.setLeveliclassification(sr.levelIclassification);
            bt.setLeveliiclassification(sr.levelIIclassification);
            bt.setLeveliiiclassification(sr.levelIIIclassification);
            bt.setLevelivclassification(sr.levelIVclassification);
            bt.setHierarchyup(sr.hierarchyup);
            bt.setHierarchydown(sr.hierarchydown);
            bt.setHierarchy0isoform(sr.hierarchy0isoform);
            bt.setHierarchy0mutation(sr.hierarchy0mutation);
            bt.setHierarchy0other(sr.hierarchy0other);
            bt.setLevelifunction(sr.levelIfunction);
            bt.setLeveligenestructure(sr.levelIgenestructure);
            bt.setDisease(sr.disease);
            bt.setRoleincancers(sr.roleincancers);
            BIOTYPESMAPPER.save(bt);
            Levelclassification lc = new Levelclassification(sr.levelIclassification, "I", sr.masterTranscriptId);
            LEVELCLASSIFICATIONMAPPER.save(lc);
            lc = new Levelclassification(sr.levelIIclassification, "II", sr.masterTranscriptId);
            LEVELCLASSIFICATIONMAPPER.save(lc);
            lc = new Levelclassification(sr.levelIIIclassification, "III", sr.masterTranscriptId);
            LEVELCLASSIFICATIONMAPPER.save(lc);
            lc = new Levelclassification(sr.levelIVclassification, "IV", sr.masterTranscriptId);
            LEVELCLASSIFICATIONMAPPER.save(lc);
            lc = new Levelclassification(sr.levelIfunction, "I_function", sr.masterTranscriptId);
            LEVELCLASSIFICATIONMAPPER.save(lc);
            lc = new Levelclassification(sr.levelIgenestructure, "I_genestructure", sr.masterTranscriptId);
            LEVELCLASSIFICATIONMAPPER.save(lc);
        }
    }

    @Override
    public void editBiotypes(String search, String replace, String user, String authToken) {
        if (authenticateUser(user, authToken)) {
            BatchStatement batch = new BatchStatement();

            BoundStatement bs = LEVELCLASSIFICATIONQUERY.bind().setString("c", search).setString("l", "I");
            List<Levelclassification> lcs = LEVELCLASSIFICATIONMAPPER.map(SESSION.execute(bs)).all();
            lcs.stream().forEach((lc) -> {
                Levelclassification _lc = new Levelclassification(replace, "I", lc.getTranscriptid());
                batch.add(LEVELCLASSIFICATIONMAPPER.saveQuery(_lc));
                batch.add(LEVELCLASSIFICATIONMAPPER.deleteQuery(lc));
                Biotypes b = BIOTYPESMAPPER.get(1, lc.getTranscriptid());
                b.setLeveliclassification(replace);
                batch.add(BIOTYPESMAPPER.saveQuery(b));
            });

            bs = LEVELCLASSIFICATIONQUERY.bind().setString("c", search).setString("l", "II");
            lcs = LEVELCLASSIFICATIONMAPPER.map(SESSION.execute(bs)).all();
            lcs.stream().forEach((lc) -> {
                Levelclassification _lc = new Levelclassification(replace, "II", lc.getTranscriptid());
                batch.add(LEVELCLASSIFICATIONMAPPER.saveQuery(_lc));
                batch.add(LEVELCLASSIFICATIONMAPPER.deleteQuery(lc));
                Biotypes b = BIOTYPESMAPPER.get(1, lc.getTranscriptid());
                b.setLeveliiclassification(replace);
                batch.add(BIOTYPESMAPPER.saveQuery(b));
            });

            bs = LEVELCLASSIFICATIONQUERY.bind().setString("c", search).setString("l", "III");
            lcs = LEVELCLASSIFICATIONMAPPER.map(SESSION.execute(bs)).all();
            lcs.stream().forEach((lc) -> {
                Levelclassification _lc = new Levelclassification(replace, "III", lc.getTranscriptid());
                batch.add(LEVELCLASSIFICATIONMAPPER.saveQuery(_lc));
                batch.add(LEVELCLASSIFICATIONMAPPER.deleteQuery(lc));
                Biotypes b = BIOTYPESMAPPER.get(1, lc.getTranscriptid());
                b.setLeveliiiclassification(replace);
                batch.add(BIOTYPESMAPPER.saveQuery(b));
            });

            bs = LEVELCLASSIFICATIONQUERY.bind().setString("c", search).setString("l", "IV");
            lcs = LEVELCLASSIFICATIONMAPPER.map(SESSION.execute(bs)).all();
            lcs.stream().forEach((lc) -> {
                Levelclassification _lc = new Levelclassification(replace, "IV", lc.getTranscriptid());
                batch.add(LEVELCLASSIFICATIONMAPPER.saveQuery(_lc));
                batch.add(LEVELCLASSIFICATIONMAPPER.deleteQuery(lc));
                Biotypes b = BIOTYPESMAPPER.get(1, lc.getTranscriptid());
                b.setLevelivclassification(replace);
                batch.add(BIOTYPESMAPPER.saveQuery(b));
            });

            bs = LEVELCLASSIFICATIONQUERY.bind().setString("c", search).setString("l", "I_function");
            lcs = LEVELCLASSIFICATIONMAPPER.map(SESSION.execute(bs)).all();
            lcs.stream().forEach((lc) -> {
                Levelclassification _lc = new Levelclassification(replace, "I_function", lc.getTranscriptid());
                batch.add(LEVELCLASSIFICATIONMAPPER.saveQuery(_lc));
                batch.add(LEVELCLASSIFICATIONMAPPER.deleteQuery(lc));
                Biotypes b = BIOTYPESMAPPER.get(1, lc.getTranscriptid());
                b.setLevelifunction(replace);
                batch.add(BIOTYPESMAPPER.saveQuery(b));
            });

            bs = LEVELCLASSIFICATIONQUERY.bind().setString("c", search).setString("l", "I_genestructure");
            lcs = LEVELCLASSIFICATIONMAPPER.map(SESSION.execute(bs)).all();
            lcs.stream().forEach((lc) -> {
                Levelclassification _lc = new Levelclassification(replace, "I_genestructure", lc.getTranscriptid());
                batch.add(LEVELCLASSIFICATIONMAPPER.saveQuery(_lc));
                batch.add(LEVELCLASSIFICATIONMAPPER.deleteQuery(lc));
                Biotypes b = BIOTYPESMAPPER.get(1, lc.getTranscriptid());
                b.setLeveligenestructure(replace);
                batch.add(BIOTYPESMAPPER.saveQuery(b));
            });
            SESSION.execute(batch);
            batch.clear();
        }
    }

    @Override
    public void editSummary(SummaryResults sr, boolean allExperiments, String user, String authToken) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSummary(int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DataStatisticsResults getDataStatistics(long experimentId, int distance) {
        DataStatisticsResults dsr = new DataStatisticsResults();
        try {
            BoundStatement bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", distance);
            ResultSet _rs = SESSION.execute(bs);
            Datastatistics ds = DATASTATISTICSMAPPER.map(_rs).one();
            dsr.totalReads = ds.getTotalreads();
            dsr.totalReadsNonRedundant = ds.getTotalreadsnonredundant();
            dsr.totalMappedReads = ds.getTotalmappedreads();
            dsr.totalMappedReadsNonRedundant = ds.getTotalmappedreadsnonredundant();

        } catch (Exception ex) {
            Logger.getLogger(AdminServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return dsr;
    }

    @Override
    public void composeOverlappingReadRegions(long experimentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean createUser(String user, String authToken, String email) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(authToken.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }

            Users _user = new Users();
            _user.setUser(user);
            _user.setPassword(sb.toString());
            _user.setEmail(email);
            USERSMAPPER.save(_user);

        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(AdminServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }

    @Override
    public boolean authenticateUser(String user, String authToken) {
        try {
            BoundStatement bs = USERSQUERY.bind().setString("us", user);
            Users _user = USERSMAPPER.map(SESSION.execute(bs)).one();
            if (_user.getPassword().equals(authToken)) {
                return true;

            }
        } catch (Exception ex) {
            Logger.getLogger(AdminServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    @Override
    public Status getServerStatus() {
        ClusterHealthStatus health = DataAccessES.getHealthStatus();
        switch (health) {
            case GREEN:
                return Status.GREEN;
            case YELLOW:
                return Status.YELLOW;
            case RED:
                return Status.RED;
            default:
                return Status.GREEN;
        }
    }
}
