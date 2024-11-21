package cache;

import cache.dataimportes.CreateExperimentSummaries;
import cache.dataimportes.CreateTranscriptAlignmentSummaries;
import cache.dataimportes.holders.AlignmentResult;
import cache.dataimportes.holders.AnnotationResults;
import cache.dataimportes.holders.SearchResult;
import cache.dataimportes.holders.Strand;
import static cache.dataimportes.holders.Strand.BOTH;
import static cache.dataimportes.holders.Strand.FORWARD;
import static cache.dataimportes.holders.Strand.REVERSECOMPLEMENT;
import cache.dataimportes.holders.TranscriptMappingResults;
import cache.dataimportes.model.Biotypes;
import cache.dataimportes.model.Datastatistics;
import cache.dataimportes.model.Experiment;
import cache.dataimportes.model.Experimentkey;
import cache.dataimportes.model.Experimentsummary;
import cache.dataimportes.model.Transcript;
import cache.dataimportes.model.Transcriptalignment;
import cache.dataimportes.model.Transcriptalignment1mismatch;
import cache.dataimportes.model.Transcriptalignment1mismatchtranspose;
import cache.dataimportes.model.Transcriptalignment2mismatch;
import cache.dataimportes.model.Transcriptalignment2mismatchtranspose;
import cache.dataimportes.model.Transcriptalignmentbartelpolya;
import cache.dataimportes.model.Transcriptalignmentbartelpolyatranspose;
import cache.dataimportes.model.Transcriptalignmentpolya;
import cache.dataimportes.model.Transcriptalignmentpolyatranspose;
import cache.dataimportes.model.Transcriptalignmentsummary;
import cache.dataimportes.model.Transcriptalignmenttranspose;
import cache.dataimportes.model.Transcriptalignmenttss;
import cache.dataimportes.model.Transcriptalignmenttsstranspose;
import cache.dataimportes.model.Transcriptannotations;
import cache.dataimportes.model.Unmappedreads;
import cache.dataimportes.model.Users;
import cache.dataimportes.util.ConsumerService;
import cache.dataimportes.util.DamerauLevenshteinEditDistance;
import static cache.dataimportes.util.DataAccess.ALLBIOTYPEQUERY;
import static cache.dataimportes.util.DataAccess.BIOTYPESMAPPER;
import static cache.dataimportes.util.DataAccess.BIOTYPESQUERY;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSMAPPER;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTKEYMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTKEYQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTSUMMARYMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTSUMMARYQUERY;
import static cache.dataimportes.util.DataAccess.MAXTRANSCRIPTIDQUERY;
import static cache.dataimportes.util.DataAccess.READSQUERY;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTBARTELPOLYAQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTPOLYAMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTPOLYAQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTSUMMARYMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTRANSPOSEQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTSSMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTSSQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTANNOTATIONSMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTANNOTATIONSQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTQUERY;
import static cache.dataimportes.util.DataAccess.UNMAPPEDREADSMAPPER;
import static cache.dataimportes.util.DataAccess.USERSMAPPER;
import static cache.dataimportes.util.DataAccess.USERSQUERY;
import cache.dataimportes.util.DataAccessES;
import static cache.dataimportes.util.DataAccessES.CLIENT;
import static cache.dataimportes.util.DataAccessES.SYNONYMSEARCHBUILDER;
import static cache.dataimportes.util.DataAccessES.TRANSCRIPTSEARCHBUILDER;
import static cache.dataimportes.util.DataAccessES.searchBartelPolyaReads;
import static cache.dataimportes.util.DataAccessES.searchPolyaReads;
import static cache.dataimportes.util.DataAccessES.searchReadsAllMatchesFuzzyScrolling;
import static cache.dataimportes.util.DataAccessES.searchReadsAllMatchesFuzzyScrolling2;
import static cache.dataimportes.util.DataAccessES.searchReadsExactMatchScrolling;
import static cache.dataimportes.util.DataAccessES.searchTssReads;
import static cache.dataimportes.util.DataAccessES.tokenize;
import cache.dataimportes.util.Email;
import cache.dataimportes.util.Fuzzy;
import cache.dataimportes.util.Stack1Mismatch;
import cache.dataimportes.util.Stack2Mismatch;
import cache.dataimportes.util.StackExactMatch;
import cache.dataimportes.util.ZipUtil;
import static cache.dataimportes.util.ZipUtil.encode;
import static cache.dataimportes.util.ZipUtil.decode;
import cache.interfaces.AlignmentService;
import com.caucho.hessian.server.HessianServlet;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import com.datastax.driver.core.utils.UUIDs;
import jaligner.Alignment;
import jaligner.Sequence;
import jaligner.SmithWatermanGotoh;
import jaligner.matrix.Matrix;
import jaligner.matrix.MatrixLoader;
import jaligner.matrix.MatrixLoaderException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import javax.validation.ConstraintViolationException;
import org.apache.commons.lang3.StringUtils;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.views.ComplementSequenceView;
import org.biojava.nbio.core.sequence.views.ReversedSequenceView;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

/**
 *
 * @author Manjunath Kustagi
 */
public class AlignmentServiceImpl extends HessianServlet implements AlignmentService {

    protected static Matrix identity;
    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();
    static final int NUM_THREADS = 20;

    static {
        LogManager.getLogManager().reset();
        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.INFO);

//        final LoggerContext ctx = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
//        final Configuration config = ctx.getConfiguration();
//        config.getLoggers().get("com.datastax.driver.core.Connection").setLevel(org.apache.logging.log4j.Level.INFO);
//        ctx.updateLoggers();
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);

        System.setProperty("org.apache.lucene.maxClauseCount", Integer.toString(Integer.MAX_VALUE));
        try {
            identity = MatrixLoader.load("IDENTITY");
        } catch (MatrixLoaderException mle) {
            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, mle);
        }
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    @Override
    public Long runAlignmentWithList(List<Long> transcripts) {
        try {

            for (Long tid : transcripts) {

                Callable runnable = (Callable) () -> {
                    try {
                        Statement stmt1 = QueryBuilder
                                .select()
                                .all()
                                .from("cache", "transcript")
                                .where(eq("key", 1))
                                .and(eq("transcriptid", tid));
                        Row row = SESSION.execute(stmt1).one();

                        String name = row.getString("name");
                        String sequence = row.getString("sequence");

                        Transcript t1 = TRANSCRIPTMAPPER.get(1, tid, name);
                        //Persist PolyA
                        ExecutorService ex = Executors.newFixedThreadPool(3, (Runnable r) -> {
                            Thread t = new Thread(r);
                            t.setPriority(Thread.MAX_PRIORITY);
                            return t;
                        });
                        Callable<List<SearchResult>> cr1 = () -> searchPolyaReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
                        Callable<List<SearchResult>> cr2 = () -> searchBartelPolyaReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
                        Callable<List<SearchResult>> cr3 = () -> searchTssReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
                        Future<List<SearchResult>> f1 = ex.submit(cr1);
                        Future<List<SearchResult>> f2 = ex.submit(cr2);
                        Future<List<SearchResult>> f3 = ex.submit(cr3);
                        List<SearchResult> polyaResults = f1.get();
                        List<SearchResult> bartelPolyaResults = f2.get();
                        List<SearchResult> tssResults = f3.get();
                        ex.shutdown();
                        if (polyaResults != null) {
                            BatchStatement batch = new BatchStatement();
                            for (SearchResult sr : polyaResults) {
                                String rs = sr.sequence;
                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                                    DNASequence _sequence = new DNASequence(sr.sequence);
                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                            = new ReversedSequenceView<>(
                                                    new ComplementSequenceView<>(_sequence));
                                    rs = revComp.getSequenceAsString();
                                }
                                String es = sequence;
                                int sc = StringUtils.indexOf(es, rs);
                                int cm = StringUtils.countMatches(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
                                    taPolya.setKey(1);
                                    taPolya.setReadid(sr.readID);
                                    taPolya.setId(UUIDs.timeBased());
                                    taPolya.setCount(sr.readCount);
                                    taPolya.setTranscriptid(tid);
                                    taPolya.setStartcoordinate(sc);
                                    taPolya.setStopcoordinate((long) (sc + rs.length()));
                                    taPolya.setScore((double) rs.length());
                                    taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, tid, sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
                                    batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                                if (cm > 1) {
                                    while (sc >= 0) {
                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                        sc = StringUtils.indexOf(es, rs);
                                        if (sc >= 0) {
                                            Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
                                            taPolya.setKey(1);
                                            taPolya.setReadid(sr.readID);
                                            taPolya.setId(UUIDs.timeBased());
                                            taPolya.setCount(sr.readCount);
                                            taPolya.setTranscriptid(tid);
                                            taPolya.setStartcoordinate(sc);
                                            taPolya.setStopcoordinate((long) (sc + rs.length()));
                                            taPolya.setScore((double) rs.length());
                                            taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                            Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, tid, sr.readID);
                                            batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
                                            batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                            if (batch.size() > 100) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            if (batch.size() > 0) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        //Persist Bartel PolyA
                        if (bartelPolyaResults != null) {
                            BatchStatement batch = new BatchStatement();
                            for (SearchResult sr : bartelPolyaResults) {
                                String rs = sr.sequence;
                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                                    DNASequence _sequence = new DNASequence(sr.sequence);
                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                            = new ReversedSequenceView<>(
                                                    new ComplementSequenceView<>(_sequence));
                                    rs = revComp.getSequenceAsString();
                                }
                                String es = sequence;
                                int sc = StringUtils.indexOf(es, rs);
                                int cm = StringUtils.countMatches(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
                                    taBPolya.setKey(1);
                                    taBPolya.setReadid(sr.readID);
                                    taBPolya.setId(UUIDs.timeBased());
                                    taBPolya.setCount(sr.readCount);
                                    taBPolya.setTranscriptid(tid);
                                    taBPolya.setStartcoordinate(sc);
                                    taBPolya.setStopcoordinate((long) (sc + rs.length()));
                                    taBPolya.setScore((double) rs.length());
                                    taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, tid, sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                                if (cm > 1) {
                                    while (sc >= 0) {
                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                        sc = StringUtils.indexOf(es, rs);
                                        if (sc >= 0) {
                                            Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
                                            taBPolya.setKey(1);
                                            taBPolya.setReadid(sr.readID);
                                            taBPolya.setId(UUIDs.timeBased());
                                            taBPolya.setCount(sr.readCount);
                                            taBPolya.setTranscriptid(tid);
                                            taBPolya.setStartcoordinate(sc);
                                            taBPolya.setStopcoordinate((long) (sc + rs.length()));
                                            taBPolya.setScore((double) rs.length());
                                            taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                            Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, tid, sr.readID);
                                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
                                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                            if (batch.size() > 100) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            if (batch.size() > 0) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        //Persist TSS
                        if (tssResults != null) {
                            BatchStatement batch = new BatchStatement();
                            for (SearchResult sr : tssResults) {
                                String rs = sr.sequence;
                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                                    DNASequence _sequence = new DNASequence(sr.sequence);
                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                            = new ReversedSequenceView<>(
                                                    new ComplementSequenceView<>(_sequence));
                                    rs = revComp.getSequenceAsString();
                                }
                                String es = sequence;
                                int sc = StringUtils.indexOf(es, rs);
                                int cm = StringUtils.countMatches(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmenttss taTss = new Transcriptalignmenttss();
                                    taTss.setKey(1);
                                    taTss.setReadid(sr.readID);
                                    taTss.setId(UUIDs.timeBased());
                                    taTss.setCount(sr.readCount);
                                    taTss.setTranscriptid(tid);
                                    taTss.setStartcoordinate(sc);
                                    taTss.setStopcoordinate((long) (sc + rs.length()));
                                    taTss.setScore((double) rs.length());
                                    taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, tid, sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
                                    batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                                if (cm > 1) {
                                    while (sc >= 0) {
                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                        sc = StringUtils.indexOf(es, rs);
                                        if (sc >= 0) {
                                            Transcriptalignmenttss taTss = new Transcriptalignmenttss();
                                            taTss.setKey(1);
                                            taTss.setReadid(sr.readID);
                                            taTss.setId(UUIDs.timeBased());
                                            taTss.setCount(sr.readCount);
                                            taTss.setTranscriptid(tid);
                                            taTss.setStartcoordinate(sc);
                                            taTss.setStopcoordinate((long) (sc + rs.length()));
                                            taTss.setScore((double) rs.length());
                                            taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                            Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, tid, sr.readID);
                                            batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
                                            batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
                                            if (batch.size() > 100) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            if (batch.size() > 0) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        if (t1.getSequence().length() >= 30000) {
                            ex = Executors.newFixedThreadPool(35, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                        } else {
                            ex = Executors.newFixedThreadPool(65, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                        }
                        // Persist Alignments
                        List<Callable<Object>> callables = new ArrayList<>();
                        Set<Experiment> experiments = new HashSet<>();
                        for (int k = 0; k < 100; k++) {
                            Statement stmt2 = QueryBuilder
                                    .select()
                                    .column("id")
                                    .column("distance")
                                    .from("cache", "experiment")
                                    .where(eq("key", k));
                            ResultSet _rs = SESSION.execute(stmt2);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                int distance1 = _row.getInt("distance");
                                if (distance1 == 0) {
                                    Long eid = _row.getLong("id");
                                    BoundStatement bs1 = EXPERIMENTQUERY.bind().setInt("ky", k).setLong("id", eid).setInt("d", 0);
                                    experiments.add(EXPERIMENTMAPPER.map(SESSION.execute(bs1)).one());
                                }
                            }
                        }
                        TranscriptMappingResults tmr = new TranscriptMappingResults();
                        tmr.transcriptID = t1.getTranscriptid();
                        tmr.name = t1.getName();
                        tmr.mappedAlignments = t1.getSequence();
                        experiments.stream().map((e) -> new CallableImplPrimaryAlignments(e, t1, tmr)).forEach((c) -> {
                            callables.add(c);
                        });
                        ex.invokeAll(callables);
                        ex.shutdown();

                        /**
                         * Send email
                         */
                        String subject = "Your alignment transcript process of " + name;
                        String to = "";
                        String message = "The process of aligning transcript, " + name + ", is complete. "
                                + "The new transcript is available from searches across all the experiments";
                        Email.sendMessage(to, subject, message);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(AlignmentServiceImpl.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                    return Boolean.TRUE;
                };
                CONSUMER_QUEUE.put(runnable);
            }
        } catch (Exception e) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, e);
        }
        return 0l;
    }

    @Override
    public Long runAlignmentWithLists(List<Long> transcripts, List<Long> experimentIds) {
        try {

            for (Long tid : transcripts) {

                Callable runnable = (Callable) () -> {
                    try {
                        Statement stmt1 = QueryBuilder
                                .select()
                                .all()
                                .from("cache", "transcript")
                                .where(eq("key", 1))
                                .and(eq("transcriptid", tid));
                        Row row = SESSION.execute(stmt1).one();

                        String name = row.getString("name");
                        String sequence = row.getString("sequence");
//                        DataAccessES.indexTranscript(tid, name, sequence);

                        Transcript t1 = TRANSCRIPTMAPPER.get(1, tid, name);
                        //Persist PolyA
                        ExecutorService ex = Executors.newFixedThreadPool(3, (Runnable r) -> {
                            Thread t = new Thread(r);
                            t.setPriority(Thread.MAX_PRIORITY);
                            return t;
                        });
//                        Callable<List<SearchResult>> cr1 = () -> searchPolyaReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
//                        Callable<List<SearchResult>> cr2 = () -> searchBartelPolyaReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
//                        Callable<List<SearchResult>> cr3 = () -> searchTssReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
//                        Future<List<SearchResult>> f1 = ex.submit(cr1);
//                        Future<List<SearchResult>> f2 = ex.submit(cr2);
//                        Future<List<SearchResult>> f3 = ex.submit(cr3);
//                        List<SearchResult> polyaResults = f1.get();
//                        List<SearchResult> bartelPolyaResults = f2.get();
//                        List<SearchResult> tssResults = f3.get();
                        ex.shutdown();
//                        if (polyaResults != null) {
//                            BatchStatement batch = new BatchStatement();
//                            for (SearchResult sr : polyaResults) {
//                                String rs = sr.sequence;
//                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
//                                    DNASequence _sequence = new DNASequence(sr.sequence);
//                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
//                                            = new ReversedSequenceView<>(
//                                                    new ComplementSequenceView<>(_sequence));
//                                    rs = revComp.getSequenceAsString();
//                                }
//                                String es = sequence;
//                                int sc = StringUtils.indexOf(es, rs);
//                                int cm = StringUtils.countMatches(es, rs);
//                                if (sc >= 0) {
//                                    Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
//                                    taPolya.setKey(1);
//                                    taPolya.setReadid(sr.readID);
//                                    taPolya.setId(UUIDs.timeBased());
//                                    taPolya.setCount(sr.readCount);
//                                    taPolya.setTranscriptid(tid);
//                                    taPolya.setStartcoordinate(sc);
//                                    taPolya.setStopcoordinate((long) (sc + rs.length()));
//                                    taPolya.setScore((double) rs.length());
//                                    taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
//                                    Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, tid, sr.readID);
//                                    batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
//                                    batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
//                                    if (batch.size() > 100) {
//                                        SESSION.execute(batch);
//                                        batch.clear();
//                                    }
//                                }
//                                if (cm > 1) {
//                                    while (sc >= 0) {
//                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
//                                        sc = StringUtils.indexOf(es, rs);
//                                        if (sc >= 0) {
//                                            Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
//                                            taPolya.setKey(1);
//                                            taPolya.setReadid(sr.readID);
//                                            taPolya.setId(UUIDs.timeBased());
//                                            taPolya.setCount(sr.readCount);
//                                            taPolya.setTranscriptid(tid);
//                                            taPolya.setStartcoordinate(sc);
//                                            taPolya.setStopcoordinate((long) (sc + rs.length()));
//                                            taPolya.setScore((double) rs.length());
//                                            taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
//                                            Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, tid, sr.readID);
//                                            batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
//                                            batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
//                                            if (batch.size() > 100) {
//                                                SESSION.execute(batch);
//                                                batch.clear();
//                                            }
//                                        }
//                                    }
//                                }
//                                if (batch.size() > 100) {
//                                    SESSION.execute(batch);
//                                    batch.clear();
//                                }
//                            }
//                            if (batch.size() > 0) {
//                                SESSION.execute(batch);
//                                batch.clear();
//                            }
//                        }
                        //Persist Bartel PolyA
//                        if (bartelPolyaResults != null) {
//                            BatchStatement batch = new BatchStatement();
//                            for (SearchResult sr : bartelPolyaResults) {
//                                String rs = sr.sequence;
//                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
//                                    DNASequence _sequence = new DNASequence(sr.sequence);
//                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
//                                            = new ReversedSequenceView<>(
//                                                    new ComplementSequenceView<>(_sequence));
//                                    rs = revComp.getSequenceAsString();
//                                }
//                                String es = sequence;
//                                int sc = StringUtils.indexOf(es, rs);
//                                int cm = StringUtils.countMatches(es, rs);
//                                if (sc >= 0) {
//                                    Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
//                                    taBPolya.setKey(1);
//                                    taBPolya.setReadid(sr.readID);
//                                    taBPolya.setId(UUIDs.timeBased());
//                                    taBPolya.setCount(sr.readCount);
//                                    taBPolya.setTranscriptid(tid);
//                                    taBPolya.setStartcoordinate(sc);
//                                    taBPolya.setStopcoordinate((long) (sc + rs.length()));
//                                    taBPolya.setScore((double) rs.length());
//                                    taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
//                                    Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, tid, sr.readID);
//                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
//                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
//                                    if (batch.size() > 100) {
//                                        SESSION.execute(batch);
//                                        batch.clear();
//                                    }
//                                }
//                                if (cm > 1) {
//                                    while (sc >= 0) {
//                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
//                                        sc = StringUtils.indexOf(es, rs);
//                                        if (sc >= 0) {
//                                            Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
//                                            taBPolya.setKey(1);
//                                            taBPolya.setReadid(sr.readID);
//                                            taBPolya.setId(UUIDs.timeBased());
//                                            taBPolya.setCount(sr.readCount);
//                                            taBPolya.setTranscriptid(tid);
//                                            taBPolya.setStartcoordinate(sc);
//                                            taBPolya.setStopcoordinate((long) (sc + rs.length()));
//                                            taBPolya.setScore((double) rs.length());
//                                            taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
//                                            Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, tid, sr.readID);
//                                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
//                                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
//                                            if (batch.size() > 100) {
//                                                SESSION.execute(batch);
//                                                batch.clear();
//                                            }
//                                        }
//                                    }
//                                }
//                                if (batch.size() > 100) {
//                                    SESSION.execute(batch);
//                                    batch.clear();
//                                }
//                            }
//                            if (batch.size() > 0) {
//                                SESSION.execute(batch);
//                                batch.clear();
//                            }
//                        }
                        //Persist TSS
//                        if (tssResults != null) {
//                            BatchStatement batch = new BatchStatement();
//                            for (SearchResult sr : tssResults) {
//                                String rs = sr.sequence;
//                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
//                                    DNASequence _sequence = new DNASequence(sr.sequence);
//                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
//                                            = new ReversedSequenceView<>(
//                                                    new ComplementSequenceView<>(_sequence));
//                                    rs = revComp.getSequenceAsString();
//                                }
//                                String es = sequence;
//                                int sc = StringUtils.indexOf(es, rs);
//                                int cm = StringUtils.countMatches(es, rs);
//                                if (sc >= 0) {
//                                    Transcriptalignmenttss taTss = new Transcriptalignmenttss();
//                                    taTss.setKey(1);
//                                    taTss.setReadid(sr.readID);
//                                    taTss.setId(UUIDs.timeBased());
//                                    taTss.setCount(sr.readCount);
//                                    taTss.setTranscriptid(tid);
//                                    taTss.setStartcoordinate(sc);
//                                    taTss.setStopcoordinate((long) (sc + rs.length()));
//                                    taTss.setScore((double) rs.length());
//                                    taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
//                                    Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, tid, sr.readID);
//                                    batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
//                                    batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
//                                    if (batch.size() > 100) {
//                                        SESSION.execute(batch);
//                                        batch.clear();
//                                    }
//                                }
//                                if (cm > 1) {
//                                    while (sc >= 0) {
//                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
//                                        sc = StringUtils.indexOf(es, rs);
//                                        if (sc >= 0) {
//                                            Transcriptalignmenttss taTss = new Transcriptalignmenttss();
//                                            taTss.setKey(1);
//                                            taTss.setReadid(sr.readID);
//                                            taTss.setId(UUIDs.timeBased());
//                                            taTss.setCount(sr.readCount);
//                                            taTss.setTranscriptid(tid);
//                                            taTss.setStartcoordinate(sc);
//                                            taTss.setStopcoordinate((long) (sc + rs.length()));
//                                            taTss.setScore((double) rs.length());
//                                            taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
//                                            Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, tid, sr.readID);
//                                            batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
//                                            batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
//                                            if (batch.size() > 100) {
//                                                SESSION.execute(batch);
//                                                batch.clear();
//                                            }
//                                        }
//                                    }
//                                }
//                                if (batch.size() > 100) {
//                                    SESSION.execute(batch);
//                                    batch.clear();
//                                }
//                            }
//                            if (batch.size() > 0) {
//                                SESSION.execute(batch);
//                                batch.clear();
//                            }
//                        }
                        if (t1.getSequence().length() >= 30000) {
                            ex = Executors.newFixedThreadPool(35, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                        } else {
                            ex = Executors.newFixedThreadPool(65, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                        }
                        // Persist Alignments
                        List<Callable<Object>> callables = new ArrayList<>();
                        Set<Experiment> experiments = new HashSet<>();
                        for (int k = 0; k < 100; k++) {
                            Statement stmt2 = QueryBuilder
                                    .select()
                                    .column("id")
                                    .column("distance")
                                    .from("cache", "experiment")
                                    .where(eq("key", k));
                            ResultSet _rs = SESSION.execute(stmt2);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                int distance1 = _row.getInt("distance");
                                if (distance1 == 0) {
                                    Long eid = _row.getLong("id");
                                    if (experimentIds.contains(eid)) {
                                        BoundStatement bs1 = EXPERIMENTQUERY.bind().setInt("ky", k).setLong("id", eid).setInt("d", 0);
                                        experiments.add(EXPERIMENTMAPPER.map(SESSION.execute(bs1)).one());
                                    }
                                }
                            }
                        }
                        TranscriptMappingResults tmr = new TranscriptMappingResults();
                        tmr.transcriptID = t1.getTranscriptid();
                        tmr.name = t1.getName();
                        tmr.mappedAlignments = t1.getSequence();
                        experiments.stream().map((e) -> new CallableImplPrimaryAlignments(e, t1, tmr)).forEach((c) -> {
                            callables.add(c);
                        });
                        ex.invokeAll(callables);
                        ex.shutdown();

//                        ex = Executors.newFixedThreadPool(65, (Runnable r) -> {
//                            Thread t = new Thread(r);
//                            t.setPriority(Thread.MAX_PRIORITY);
//                            return t;
//                        });
//
//                        callables.clear();
//                        experiments.stream().map((e) -> new CallableImplSecondaryAlignments(e, t1, tmr)).forEach((c) -> {
//                            callables.add(c);
//                        });
//                        ex.invokeAll(callables);
//                        ex.shutdown();
                        /**
                         * Send email
                         */
                        String subject = "Your alignment transcript process of " + name;
                        String to = "";
                        String message = "The process of aligning transcript, " + name + ", is complete. "
                                + "The new transcript is available from searches across all the experiments";
                        Email.sendMessage(to, subject, message);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(AlignmentServiceImpl.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                    return Boolean.TRUE;
                };
                CONSUMER_QUEUE.put(runnable);
            }
        } catch (Exception e) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, e);
        }
        return 0l;

    }

    @Override
    public void stopAlignment(long token) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getAlignedSequences(int alignmentId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List search(String r, boolean rc) {
        final List<SearchResult> searchResults = Collections.synchronizedList(new ArrayList<>());

        if (rc) {
            try {
                DNASequence sequence = new DNASequence(r);
                org.biojava.nbio.core.sequence.template.Sequence _rc
                        = new ReversedSequenceView<>(
                                new ComplementSequenceView<>(sequence));
                r = _rc.getSequenceAsString();
            } catch (CompoundNotFoundException ex) {
                Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        SearchResponse response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("name", r.toLowerCase())).get();
        for (SearchHit hit : response.getHits().getHits()) {
            String _n = hit.field("transcriptid").getValue().toString();
            SearchResult searchResult = new SearchResult();
            searchResult.transcriptID = Long.parseLong(_n);
            searchResult.searchLength = 101;
            if (rc) {
                searchResult.strand = Strand.REVERSECOMPLEMENT;
            } else {
                searchResult.strand = Strand.FORWARD;
            }
            BoundStatement bs = TRANSCRIPTQUERY.bind().setLong("ti", Long.parseLong(_n));
            Transcript transcript = TRANSCRIPTMAPPER.map(SESSION.execute(bs)).one();
            searchResult.sequence = transcript.getSequence();
            searchResult.name = transcript.getName();
            searchResult.type = SearchResult.SearchType.TRANSCRIPTS;
            if (!searchResults.contains(searchResult)) {
                searchResults.add(searchResult);
            }
        }

        return searchResults;
    }

    @Override
    public int getTranscriptCountForGene(String gene) {
        List<Long> names = new ArrayList<>();
        SearchResponse response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("name", gene)).get();
        for (SearchHit hit : response.getHits().getHits()) {
            Long _n = Long.parseLong(hit.field("transcriptid").getValue().toString());
            names.add(_n);
        }
        return names.size();
    }

    @Override
    public List<String> getGenesByPartialSymbol(String partialGene) {
        List<String> names = new ArrayList<>();
        try {
            partialGene = partialGene.replaceAll("'", "''");
            SearchResponse response = TRANSCRIPTSEARCHBUILDER.suggest(
                    new SuggestBuilder().addSuggestion("suggest",
                            SuggestBuilders.completionSuggestion("suggest").text(partialGene.toLowerCase()).size(20))).get();

            CompletionSuggestion compSuggestion = response.getSuggest().getSuggestion("suggest");

            List<CompletionSuggestion.Entry> entryList = compSuggestion.getEntries();
            if (entryList != null) {
                CompletionSuggestion.Entry entry = entryList.get(0);
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                if (options != null) {
                    options.stream().filter((option) -> (!names.contains(option.getText().string()))).forEach((option) -> {
                        names.add(option.getText().string());
                    });
                }
            }

            response = SYNONYMSEARCHBUILDER.suggest(
                    new SuggestBuilder().addSuggestion("suggest",
                            SuggestBuilders.completionSuggestion("suggest").text(partialGene.toLowerCase()).size(20))).get();

            compSuggestion = response.getSuggest().getSuggestion("suggest");

            entryList = compSuggestion.getEntries();
            if (entryList != null) {
                CompletionSuggestion.Entry entry = entryList.get(0);
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                if (options != null) {
                    options.stream().filter((option) -> (!names.contains(option.getText().string()))).forEach((option) -> {
                        names.add(option.getText().string());
                    });
                }
            }

//            SearchResponse response;
//            response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.prefixQuery("name", partialGene.toLowerCase())).get();
//            for (SearchHit hit : response.getHits().getHits()) {
//                String name = hit.field("name").value().toString();
//                if (!names.contains(name)) {
//                    names.add(name);
//                }
//            }
//            response = SYNONYMSEARCHBUILDER.setQuery(QueryBuilders.prefixQuery("synonym", partialGene.toLowerCase())).get();
//            for (SearchHit hit : response.getHits().getHits()) {
//                String name = hit.field("synonym").value().toString();
//                if (!names.contains(name)) {
//                    names.add(name);
//                }
//            }
        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);

        }
        return names;
    }

    @Override
    public List<String> getDistinctBiotypes() {
        Set<String> biotypes = new HashSet<>();

        BoundStatement bs = ALLBIOTYPEQUERY.bind();
        ResultSet _rs = SESSION.execute(bs);
        while (_rs.isExhausted()) {
            Row r = _rs.one();
            biotypes.add(r.getString("leveliclassification"));
            biotypes.add(r.getString("levelifunction"));
            biotypes.add(r.getString("leveligenestructure"));
            biotypes.add(r.getString("leveliiclassification"));
            biotypes.add(r.getString("leveliiiclassification"));
            biotypes.add(r.getString("levelivclassification"));
        }
        return new ArrayList<>(biotypes);
    }

    @Override
    public List<String> getGenesForBiotypes(List<String> biotypes) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//        List<String> names = new ArrayList<>();
//        biotypes.stream().map((name) -> ALLBIOTYPESLIKEQUERY.bind().setString("bt", name)).map((bs) -> SESSION.execute(bs)).forEach((ResultSet _rs) -> {
//            while (_rs.isExhausted()) {
//                String symbol = _rs.one().getString("symbol");
//                names.add(symbol);
//            }
//        });
//        return names;
    }

    @Override
    public void createOrDeleteAnnotation(List<AnnotationResults> arList, boolean create) {
        if (create) {
            arList.stream().forEach((ar) -> {
                Transcriptannotations ta = new Transcriptannotations();
                ta.setKey(1);
                ta.setId(UUIDs.timeBased());
                ta.setTranscriptid(ar.transcriptId);
                ta.setSequence(ar.sequence);
                ta.setAnnotation(ar.annotation);
                ta.setVariant(ar.variant);
                ta.setStartcoordinate((long) ar.startCoordinate);
                ta.setStopcoordinate((long) ar.stopCoordinate);
                ta.setReversecomplement(!ar.strand.equals(Strand.FORWARD));
                ta.setPredicted(ar.predicted);
                TRANSCRIPTANNOTATIONSMAPPER.save(ta);
            });
        } else {
            arList.stream().forEach((ar) -> {
                Statement stmt1 = QueryBuilder
                        .select()
                        .all()
                        .from("cache", "transcriptannotations")
                        .where(eq("key", 1))
                        .and(eq("transcriptid", ar.transcriptId))
                        .and(eq("annotation", ar.annotation))
                        .and(eq("startcoordinate", ar.startCoordinate))
                        .and(eq("stopcoordinate", ar.stopCoordinate));
                ResultSet _rs = SESSION.execute(stmt1);
                Transcriptannotations ta = TRANSCRIPTANNOTATIONSMAPPER.map(_rs).one();
                TRANSCRIPTANNOTATIONSMAPPER.delete(ta);
            });
        }
    }

    @Override
    public List<AnnotationResults> getAnnotationsForTranscript(long transcriptId) {
        List<AnnotationResults> results = new ArrayList<>();
        try {
            BoundStatement bs = TRANSCRIPTANNOTATIONSQUERY.bind().setLong("ti", transcriptId);
            ResultSet _rs = SESSION.execute(bs);
            List<Transcriptannotations> tas = TRANSCRIPTANNOTATIONSMAPPER.map(_rs).all();
            tas.stream().map((ta) -> {
                Strand strand = (!ta.getReversecomplement()) ? Strand.FORWARD : Strand.REVERSECOMPLEMENT;
                AnnotationResults ar = new AnnotationResults(ta.getId(), ta.getTranscriptid(), ta.getSequence(), ta.getAnnotation(), ta.getVariant(), ta.getStartcoordinate(), ta.getStopcoordinate(), strand, ta.getPredicted());
                return ar;
            }).forEach((ar) -> {
                results.add(ar);
            });
        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return results;
    }

    @Override
    public void deleteTranscript(final long transcriptId, String authToken) {
        try {

            BoundStatement bs = USERSQUERY.bind().setString("us", "admin");
            Users user = USERSMAPPER.map(SESSION.execute(bs)).one();

            if (user.getPassword().equals(authToken)) {
                Callable r = (Callable) () -> {
                    try {
                        Set<Long> eids1 = new HashSet<>();
                        for (int k = 0; k < 100; k++) {
                            Statement stmt2 = QueryBuilder
                                    .select()
                                    .column("id")
                                    .from("cache", "experiment")
                                    .where(eq("key", k));
                            ResultSet _rs1 = SESSION.execute(stmt2);
                            while (!_rs1.isExhausted()) {
                                Long eid = _rs1.one().getLong("id");
                                if (!eids1.contains(eid)) {
                                    eids1.add(eid);
                                }
                            }
                        }

                        BoundStatement _bs = TRANSCRIPTQUERY.bind().setLong("ti", transcriptId);
                        Transcript transcript = TRANSCRIPTMAPPER.map(SESSION.execute(_bs)).one();

                        String _tName = "";
                        if (transcript != null) {
                            _tName = transcript.getName();
                        }
                        final String tName = _tName;
                        Callable r1 = (Callable) () -> {
                            ExecutorService ex = Executors.newFixedThreadPool(10, (Runnable _r) -> {
                                Thread t = new Thread(_r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                            List<Callable<Object>> callables = new ArrayList<>();
                            eids1.stream().map((eid) -> new CallableImplDeleteAlignments(eid, transcriptId)).forEach((cda) -> {
                                callables.add(cda);
                            });
                            ex.invokeAll(callables);
                            ex.shutdown();
                            return Boolean.TRUE;
                        };

                        Callable r2 = (Callable) () -> {
                            try {
                                BatchStatement batch = new BatchStatement();
                                eids1.stream().map((eid) -> {
                                    Set<Long> multimappers = new HashSet<>();
                                    Transcriptalignmentsummary tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 0, transcriptId);
                                    multimappers.add(transcriptId);
                                    if (tas != null && tas.getMultimappers() != null) {
                                        try {
                                            List mm = (List) decode(ZipUtil.extractBytes(tas.getMultimappers().array()));
                                            Map<Integer, List<Long>> multiMappers = (Map<Integer, List<Long>>) mm.get(0);
                                            Map<Integer, List<Long>> rcMultiMappers = (Map<Integer, List<Long>>) mm.get(1);
                                            multiMappers.keySet().stream().map((key) -> multiMappers.get(key)).forEach((_tis) -> {
                                                _tis.stream().forEach((_t) -> {
                                                    multimappers.add(_t);
                                                });
                                            });
                                            rcMultiMappers.keySet().stream().map((key) -> rcMultiMappers.get(key)).forEach((_tis) -> {
                                                _tis.stream().forEach((_t) -> {
                                                    multimappers.add(_t);
                                                });
                                            });
                                        } catch (IOException | ClassNotFoundException | DataFormatException ex) {
                                            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                                        }
                                    }
                                    multimappers.stream().forEach((tid) -> {
                                        batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(eid, 0, tid));
                                        if (batch.size() > 100) {
                                            SESSION.execute(batch);
                                            batch.clear();
                                        }
                                    });
                                    return eid;
                                }).map((eid) -> {
                                    Set<Long> multimappers = new HashSet<>();
                                    Transcriptalignmentsummary tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 1, transcriptId);
                                    multimappers.add(transcriptId);
                                    if (tas != null && tas.getMultimappers() != null) {
                                        try {
                                            List mm = (List) decode(ZipUtil.extractBytes(tas.getMultimappers().array()));
                                            Map<Integer, List<Long>> multiMappers = (Map<Integer, List<Long>>) mm.get(0);
                                            Map<Integer, List<Long>> rcMultiMappers = (Map<Integer, List<Long>>) mm.get(1);
                                            multiMappers.keySet().stream().map((key) -> multiMappers.get(key)).forEach((_tis) -> {
                                                _tis.stream().forEach((_t) -> {
                                                    multimappers.add(_t);
                                                });
                                            });
                                            rcMultiMappers.keySet().stream().map((key) -> rcMultiMappers.get(key)).forEach((_tis) -> {
                                                _tis.stream().forEach((_t) -> {
                                                    multimappers.add(_t);
                                                });
                                            });
                                        } catch (IOException | ClassNotFoundException | DataFormatException ex) {
                                            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                                        }
                                    }
                                    multimappers.stream().forEach((tid) -> {
                                        batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(eid, 1, tid));
                                        if (batch.size() > 100) {
                                            SESSION.execute(batch);
                                            batch.clear();
                                        }
                                    });
                                    return eid;
                                }).forEach((eid) -> {
                                    Set<Long> multimappers = new HashSet<>();
                                    Transcriptalignmentsummary tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 2, transcriptId);
                                    multimappers.add(transcriptId);
                                    if (tas != null && tas.getMultimappers() != null) {
                                        try {
                                            List mm = (List) decode(ZipUtil.extractBytes(tas.getMultimappers().array()));
                                            Map<Integer, List<Long>> multiMappers = (Map<Integer, List<Long>>) mm.get(0);
                                            Map<Integer, List<Long>> rcMultiMappers = (Map<Integer, List<Long>>) mm.get(1);
                                            multiMappers.keySet().stream().map((key) -> multiMappers.get(key)).forEach((_tis) -> {
                                                _tis.stream().forEach((_t) -> {
                                                    multimappers.add(_t);
                                                });
                                            });
                                            rcMultiMappers.keySet().stream().map((key) -> rcMultiMappers.get(key)).forEach((_tis) -> {
                                                _tis.stream().forEach((_t) -> {
                                                    multimappers.add(_t);
                                                });
                                            });
                                        } catch (IOException | ClassNotFoundException | DataFormatException ex) {
                                            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                                        }
                                    }
                                    multimappers.stream().forEach((tid) -> {
                                        batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(eid, 2, tid));
                                        if (batch.size() > 100) {
                                            SESSION.execute(batch);
                                            batch.clear();
                                        }
                                    });
                                });
                                BoundStatement _bs1 = TRANSCRIPTALIGNMENTBARTELPOLYAQUERY.bind().setLong("ti", transcriptId);
                                List<Transcriptalignmentbartelpolya> talbp = TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.map(SESSION.execute(_bs1)).all();

                                talbp.stream().filter((ta) -> (ta != null)).map((ta) -> {
                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.deleteQuery(ta));
                                    return ta;
                                }).map((ta) -> {
                                    if (ta != null && ta.getReadid() != null && ta.getTranscriptid() != 0) {
                                        batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.deleteQuery(1l, ta.getReadid(), ta.getTranscriptid()));
                                    }
                                    return ta;
                                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                                    SESSION.execute(batch);
                                    return _item;
                                }).forEach((_item) -> {
                                    batch.clear();
                                });

                                _bs1 = TRANSCRIPTALIGNMENTPOLYAQUERY.bind().setLong("ti", transcriptId);
                                List<Transcriptalignmentpolya> talp = TRANSCRIPTALIGNMENTPOLYAMAPPER.map(SESSION.execute(_bs1)).all();

                                talp.stream().filter((ta) -> (ta != null)).map((ta) -> {
                                    batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.deleteQuery(ta));
                                    return ta;
                                }).map((ta) -> {
                                    if (ta != null && ta.getReadid() != null && ta.getTranscriptid() != 0) {
                                        batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.deleteQuery(1l, ta.getReadid(), ta.getTranscriptid()));
                                    }
                                    return ta;
                                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                                    SESSION.execute(batch);
                                    return _item;
                                }).forEach((_item) -> {
                                    batch.clear();
                                });

                                _bs1 = TRANSCRIPTALIGNMENTTSSQUERY.bind().setLong("ti", transcriptId);
                                List<Transcriptalignmenttss> talt = TRANSCRIPTALIGNMENTTSSMAPPER.map(SESSION.execute(_bs1)).all();
                                talt.stream().filter((ta) -> (ta != null)).map((ta) -> {
                                    batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.deleteQuery(ta));
                                    return ta;
                                }).map((ta) -> {
                                    if (ta != null && ta.getReadid() != null && ta.getTranscriptid() != 0) {
                                        batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.deleteQuery(1l, ta.getReadid(), ta.getTranscriptid()));
                                    }
                                    return ta;
                                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                                    SESSION.execute(batch);
                                    return _item;
                                }).forEach((_item) -> {
                                    batch.clear();
                                });

                                _bs1 = TRANSCRIPTANNOTATIONSQUERY.bind().setLong("ti", transcriptId);
                                List<Transcriptannotations> tann = TRANSCRIPTANNOTATIONSMAPPER.map(SESSION.execute(_bs1)).all();
                                tann.stream().forEach((ta) -> {
                                    if (ta != null) {
                                        batch.add(TRANSCRIPTANNOTATIONSMAPPER.deleteQuery(ta));
                                        if (batch.size() > 100) {
                                            SESSION.execute(batch);
                                            batch.clear();
                                        }
                                    }
                                });
                                for (Long eid : eids1) {
                                    _bs1 = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", eid).setInt("d", 0).setLong("ti", transcriptId);
                                    Experimentsummary es = EXPERIMENTSUMMARYMAPPER.map(SESSION.execute(_bs1)).one();
                                    if (es != null) {
                                        batch.add(EXPERIMENTSUMMARYMAPPER.deleteQuery(es));
                                    }

                                    _bs1 = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", eid).setInt("d", 1).setLong("ti", transcriptId);
                                    es = EXPERIMENTSUMMARYMAPPER.map(SESSION.execute(_bs1)).one();
                                    if (es != null) {
                                        batch.add(EXPERIMENTSUMMARYMAPPER.deleteQuery(es));
                                    }

                                    _bs1 = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", eid).setInt("d", 2).setLong("ti", transcriptId);
                                    es = EXPERIMENTSUMMARYMAPPER.map(SESSION.execute(_bs1)).one();
                                    if (es != null) {
                                        batch.add(EXPERIMENTSUMMARYMAPPER.deleteQuery(es));
                                        if (batch.size() > 100) {
                                            SESSION.execute(batch);
                                            batch.clear();
                                        }
                                    }
                                }
                                batch.add(BIOTYPESMAPPER.deleteQuery(1, transcriptId));
                                batch.add(TRANSCRIPTMAPPER.deleteQuery(1, transcriptId, tName));
                                SESSION.execute(batch);
                                batch.clear();

                                DataAccessES.deleteTranscript(transcriptId);

                            } catch (Exception ex) {
                                Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            return Boolean.TRUE;
                        };

                        ExecutorService ex = Executors.newFixedThreadPool(2, (Runnable r3) -> {
                            Thread t = new Thread(r3);
                            t.setPriority(Thread.MAX_PRIORITY);
                            return t;
                        });
                        List<Callable<Object>> callables = new ArrayList<>();
                        callables.add(r1);
                        callables.add(r2);
                        ex.invokeAll(callables);
                        ex.shutdown();

                        /**
                         * Send email
                         */
                        String subject = "Transcript deleted";
                        String to = user.getEmail();
                        String message = "Transcript: " + tName + " with id: " + transcriptId + " was deleted";
                        Email.sendMessage(to, subject, message);

                    } catch (Exception ex) {
                        Logger.getLogger(AlignmentServiceImpl.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                    return Boolean.TRUE;
                };

                if (CONSUMER_QUEUE.size() > 0) {
                    /**
                     * Send email
                     */
                    String subject = "Your delete transcript process of " + transcriptId + " is queued.";
                    String to = user.getEmail();
                    String message = "The process of deleting the transcript with id: " + transcriptId + ", is queued behind " + (CONSUMER_QUEUE.size() + 1) + " jobs.";
                    Email.sendMessage(to, subject, message);
                }

                CONSUMER_QUEUE.put(r);
            }
        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void editTranscriptName(long transcriptId, String newName, String authToken) {
        try {

            BoundStatement bs = USERSQUERY.bind().setString("us", "admin");
            Users user = USERSMAPPER.map(SESSION.execute(bs)).one();

            if (user.getPassword().equals(authToken)) {

                Callable r = (Callable) () -> {
                    try {
                        BatchStatement batch = new BatchStatement();
                        BoundStatement _bs = TRANSCRIPTQUERY.bind().setLong("ti", transcriptId);
                        Transcript transcript = TRANSCRIPTMAPPER.map(SESSION.execute(_bs)).one();

                        SearchResponse response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("name", newName)).get();

                        String _newName1 = newName;

                        if (response.getHits().getHits().length > 0) {
                            _newName1 += "_" + System.currentTimeMillis();
                        }

                        batch.add(TRANSCRIPTMAPPER.deleteQuery(transcript));
                        transcript.setname(_newName1);
                        batch.add(TRANSCRIPTMAPPER.saveQuery(transcript));

                        final String _newName2 = _newName1;

                        Set<Long> eids1 = new HashSet<>();
                        _bs = EXPERIMENTKEYQUERY.bind();
                        ResultSet _rs1 = SESSION.execute(_bs);
                        while (!_rs1.isExhausted()) {
                            eids1.add(_rs1.one().getLong("experimentid"));
                        }
                        eids1.stream().map((eid) -> {
                            Transcriptalignmentsummary tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 0, transcriptId);
                            if (tas != null) {
                                batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(tas));
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            return eid;
                        }).map((eid) -> {
                            Transcriptalignmentsummary tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 1, transcriptId);
                            if (tas != null) {
                                batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(tas));
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            return eid;
                        }).forEach((eid) -> {
                            Transcriptalignmentsummary tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 2, transcriptId);
                            if (tas != null) {
                                batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(tas));
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                        });

                        eids1.stream().forEach((eid) -> {
                            BoundStatement _bs1 = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", eid).setInt("d", 0).setLong("ti", transcriptId);
                            Experimentsummary es = EXPERIMENTSUMMARYMAPPER.map(SESSION.execute(_bs1)).one();
                            if (es != null) {
                                batch.add(EXPERIMENTSUMMARYMAPPER.deleteQuery(es));
                                es.setSymbol(_newName2);
                                batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(es));
                            }

                            _bs1 = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", eid).setInt("d", 1).setLong("ti", transcriptId);
                            es = EXPERIMENTSUMMARYMAPPER.map(SESSION.execute(_bs1)).one();
                            if (es != null) {
                                batch.add(EXPERIMENTSUMMARYMAPPER.deleteQuery(es));
                                es.setSymbol(_newName2);
                                batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(es));
                            }

                            _bs1 = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", eid).setInt("d", 2).setLong("ti", transcriptId);
                            es = EXPERIMENTSUMMARYMAPPER.map(SESSION.execute(_bs1)).one();
                            if (es != null) {
                                batch.add(EXPERIMENTSUMMARYMAPPER.deleteQuery(es));
                                es.setSymbol(_newName2);
                                batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(es));
                            }
                            if (batch.size() > 100) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        });

                        _bs = BIOTYPESQUERY.bind().setLong("ti", transcriptId);
                        List<Biotypes> biotypes = BIOTYPESMAPPER.map(SESSION.execute(_bs)).all();

                        biotypes.stream().map((b) -> {
                            b.setSymbol(_newName2);
                            return b;
                        }).map((b) -> {
                            batch.add(BIOTYPESMAPPER.saveQuery(b));
                            return b;
                        }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                            SESSION.execute(batch);
                            return _item;
                        }).forEach((_item) -> {
                            batch.clear();
                        });

                        if (batch.size() > 0) {
                            SESSION.execute(batch);
                            batch.clear();
                        }
                        DataAccessES.editTranscriptName(transcriptId, _newName2, transcript.getSequence());

                    } catch (Exception ex) {
                        Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    return Boolean.TRUE;
                };
                CONSUMER_QUEUE.put(r);

            }
        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void beginEditTranscript(long transcriptId, String authToken) {
        try {

            BoundStatement bs = USERSQUERY.bind().setString("us", "admin");
            Users user = USERSMAPPER.map(SESSION.execute(bs)).one();

            if (user.getPassword().equals(authToken)) {
                Callable r = (Callable) () -> {
                    try {
                        BoundStatement _bs = TRANSCRIPTQUERY.bind().setLong("ti", transcriptId);
                        Transcript transcript = TRANSCRIPTMAPPER.map(SESSION.execute(_bs)).one();

                        final String tName = transcript.getName();
                        /**
                         * Send email
                         */
                        String subject = "Transcript edited";
                        String to = user.getEmail();
                        String message = "Transcript: " + tName + " with id: " + transcriptId + " was authorized to be edited";
                        Email.sendMessage(to, subject, message);

                    } catch (Exception ex) {
                        Logger.getLogger(AlignmentServiceImpl.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                    return Boolean.TRUE;
                };
                CONSUMER_QUEUE.put(r);

            }
        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public Long runAlignment(final int tid, final long experimentId, final int distance) {
        Callable runnable = (Callable) () -> {
            try {

                Statement stmt1 = QueryBuilder
                        .select()
                        .all()
                        .from("cache", "transcript")
                        .where(eq("key", 1))
                        .and(eq("transcriptid", tid));
                Row row = SESSION.execute(stmt1).one();

                String name = row.getString("name");
                String sequence = row.getString("sequence");
                long tid1 = (long) tid;
//                DataAccessES.indexTranscript(tid1, name, sequence);

//                Transcript t1 = TRANSCRIPTMAPPER.get(1, tid, name);
                System.out.println("Retrieved transcript");
                //Persist PolyA
                ExecutorService ex = Executors.newFixedThreadPool(3, (Runnable r) -> {
                    Thread t = new Thread(r);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });
                System.out.println("Created Executor Service");
                Callable<List<SearchResult>> cr1 = () -> searchPolyaReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
                Callable<List<SearchResult>> cr2 = () -> searchBartelPolyaReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
                Callable<List<SearchResult>> cr3 = () -> searchTssReads(sequence, 20, SearchResult.SearchType.READS, Strand.BOTH);
                System.out.println("Submitting PolyaA search");
                Future<List<SearchResult>> f1 = ex.submit(cr1);
                System.out.println("Submitting Bartel PolyaA search");
                Future<List<SearchResult>> f2 = ex.submit(cr2);
                System.out.println("Submitting TSS search");
                Future<List<SearchResult>> f3 = ex.submit(cr3);
                List<SearchResult> polyaResults = f1.get();
                System.out.println("PolyaA search result size: " + polyaResults.size());
                List<SearchResult> bartelPolyaResults = f2.get();
                System.out.println("Bartel PolyaA search result size: " + bartelPolyaResults.size());
                List<SearchResult> tssResults = f3.get();
                System.out.println("YSS search result size: " + tssResults.size());
                ex.shutdown();
                if (polyaResults != null) {
                    BatchStatement batch = new BatchStatement();
                    for (SearchResult sr : polyaResults) {
                        String rs = sr.sequence;
                        if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                            DNASequence _sequence = new DNASequence(sr.sequence);
                            org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                    = new ReversedSequenceView<>(
                                            new ComplementSequenceView<>(_sequence));
                            rs = revComp.getSequenceAsString();
                        }
                        String es = sequence;
                        int sc = StringUtils.indexOf(es, rs);
                        int cm = StringUtils.countMatches(es, rs);
                        if (sc >= 0) {
                            Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
                            taPolya.setKey(1);
                            taPolya.setReadid(sr.readID);
                            taPolya.setId(UUIDs.timeBased());
                            taPolya.setCount(sr.readCount);
                            taPolya.setTranscriptid(tid);
                            taPolya.setStartcoordinate(sc);
                            taPolya.setStopcoordinate((long) (sc + rs.length()));
                            taPolya.setScore((double) rs.length());
                            taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                            Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, tid, sr.readID);
                            batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
                            batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
                            if (batch.size() > 100) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        if (cm > 1) {
                            while (sc >= 0) {
                                es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                sc = StringUtils.indexOf(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
                                    taPolya.setKey(1);
                                    taPolya.setReadid(sr.readID);
                                    taPolya.setId(UUIDs.timeBased());
                                    taPolya.setCount(sr.readCount);
                                    taPolya.setTranscriptid(tid);
                                    taPolya.setStartcoordinate(sc);
                                    taPolya.setStopcoordinate((long) (sc + rs.length()));
                                    taPolya.setScore((double) rs.length());
                                    taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, tid, sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
                                    batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                            }
                        }
                        if (batch.size() > 100) {
                            SESSION.execute(batch);
                            batch.clear();
                        }
                    }
                    if (batch.size() > 0) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                }
                //Persist Bartel PolyA
                if (bartelPolyaResults != null) {
                    BatchStatement batch = new BatchStatement();
                    for (SearchResult sr : bartelPolyaResults) {
                        String rs = sr.sequence;
                        if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                            DNASequence _sequence = new DNASequence(sr.sequence);
                            org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                    = new ReversedSequenceView<>(
                                            new ComplementSequenceView<>(_sequence));
                            rs = revComp.getSequenceAsString();
                        }
                        String es = sequence;
                        int sc = StringUtils.indexOf(es, rs);
                        int cm = StringUtils.countMatches(es, rs);
                        if (sc >= 0) {
                            Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
                            taBPolya.setKey(1);
                            taBPolya.setReadid(sr.readID);
                            taBPolya.setId(UUIDs.timeBased());
                            taBPolya.setCount(sr.readCount);
                            taBPolya.setTranscriptid(tid);
                            taBPolya.setStartcoordinate(sc);
                            taBPolya.setStopcoordinate((long) (sc + rs.length()));
                            taBPolya.setScore((double) rs.length());
                            taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                            Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, tid, sr.readID);
                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
                            if (batch.size() > 100) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        if (cm > 1) {
                            while (sc >= 0) {
                                es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                sc = StringUtils.indexOf(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
                                    taBPolya.setKey(1);
                                    taBPolya.setReadid(sr.readID);
                                    taBPolya.setId(UUIDs.timeBased());
                                    taBPolya.setCount(sr.readCount);
                                    taBPolya.setTranscriptid(tid);
                                    taBPolya.setStartcoordinate(sc);
                                    taBPolya.setStopcoordinate((long) (sc + rs.length()));
                                    taBPolya.setScore((double) rs.length());
                                    taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, tid, sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                            }
                        }
                        if (batch.size() > 100) {
                            SESSION.execute(batch);
                            batch.clear();
                        }
                    }
                    if (batch.size() > 0) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                }
                //Persist TSS
                if (tssResults != null) {
                    BatchStatement batch = new BatchStatement();
                    for (SearchResult sr : tssResults) {
                        String rs = sr.sequence;
                        if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                            DNASequence _sequence = new DNASequence(sr.sequence);
                            org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                    = new ReversedSequenceView<>(
                                            new ComplementSequenceView<>(_sequence));
                            rs = revComp.getSequenceAsString();
                        }
                        String es = sequence;
                        int sc = StringUtils.indexOf(es, rs);
                        int cm = StringUtils.countMatches(es, rs);
                        if (sc >= 0) {
                            Transcriptalignmenttss taTss = new Transcriptalignmenttss();
                            taTss.setKey(1);
                            taTss.setReadid(sr.readID);
                            taTss.setId(UUIDs.timeBased());
                            taTss.setCount(sr.readCount);
                            taTss.setTranscriptid(tid);
                            taTss.setStartcoordinate(sc);
                            taTss.setStopcoordinate((long) (sc + rs.length()));
                            taTss.setScore((double) rs.length());
                            taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                            Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, tid, sr.readID);
                            batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
                            batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
                            if (batch.size() > 100) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        if (cm > 1) {
                            while (sc >= 0) {
                                es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                sc = StringUtils.indexOf(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmenttss taTss = new Transcriptalignmenttss();
                                    taTss.setKey(1);
                                    taTss.setReadid(sr.readID);
                                    taTss.setId(UUIDs.timeBased());
                                    taTss.setCount(sr.readCount);
                                    taTss.setTranscriptid(tid);
                                    taTss.setStartcoordinate(sc);
                                    taTss.setStopcoordinate((long) (sc + rs.length()));
                                    taTss.setScore((double) rs.length());
                                    taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, tid, sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
                                    batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                            }
                        }
                        if (batch.size() > 100) {
                            SESSION.execute(batch);
                            batch.clear();
                        }
                    }
                    if (batch.size() > 0) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                }

                ex = Executors.newFixedThreadPool(1, (Runnable r) -> {
                    Thread t = new Thread(r);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });

                Experimentkey key = EXPERIMENTKEYMAPPER.get(experimentId, 0);

                BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", key.getKey()).setLong("id", experimentId).setInt("d", 0);
                ResultSet _rs = SESSION.execute(bs);
                Experiment experiment = EXPERIMENTMAPPER.map(_rs).one();

                bs = TRANSCRIPTQUERY.bind().setLong("ti", (long) tid);
                Transcript t1 = TRANSCRIPTMAPPER.map(SESSION.execute(bs)).one();

                TranscriptMappingResults tmr = new TranscriptMappingResults();
                tmr.transcriptID = t1.getTranscriptid();
                tmr.name = t1.getName();
                tmr.mappedAlignments = t1.getSequence();

                List<Callable<Object>> callables = new ArrayList<>();
                Callable c = new CallableImplPrimaryAlignments(experiment, t1, tmr);
                callables.add(c);
                ex.invokeAll(callables);
                ex.shutdown();

                /**
                 * Send email
                 */
                String subject = "Your alignment transcript process of " + t1.getName();
                bs = USERSQUERY.bind().setString("us", "admin");
                Users user = USERSMAPPER.map(SESSION.execute(bs)).one();
                String to = user.getEmail();
                String message = "The process of aligning transcript, " + t1.getName() + ", is complete. "
                        + "The new transcript is available from searches across all the experiments";
                Email.sendMessage(to, subject, message);
            } catch (InterruptedException ex1) {
                Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex1);
            }
            return Boolean.TRUE;
        };
        try {
            CONSUMER_QUEUE.put(runnable);
        } catch (InterruptedException ex) {
            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return experimentId;
    }

    @Override
    public AlignmentResult getAlignment(int alignmentId, int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getAlignmentCount(int key, long experimentId, int distance) {
        long count = 0;

        switch (distance) {
            case 0:
                Statement stmt1 = QueryBuilder
                        .select()
                        .countAll()
                        .from("cache", "transcriptalignment")
                        .where(eq("experimentid", experimentId));
                ResultSet _rs = SESSION.execute(stmt1);
                count = _rs.one().getLong(0);
                break;
            case 1:
                stmt1 = QueryBuilder
                        .select()
                        .countAll()
                        .from("cache", "transcriptalignment1mismatch")
                        .where(eq("experimentid", experimentId));
                _rs = SESSION.execute(stmt1);
                count = _rs.one().getLong(0);
                break;
            case 2:
                stmt1 = QueryBuilder
                        .select()
                        .countAll()
                        .from("cache", "transcriptalignment2mismatch")
                        .where(eq("experimentid", experimentId));
                _rs = SESSION.execute(stmt1);
                count = _rs.one().getLong(0);
                break;
            default:
                stmt1 = QueryBuilder
                        .select()
                        .countAll()
                        .from("cache", "transcriptalignment")
                        .where(eq("experimentid", experimentId));
                _rs = SESSION.execute(stmt1);
                count = _rs.one().getLong(0);
                break;
        }

        return count;
    }

    @Override
    public Map<Long, List<Long>> loadMappedTranscripts(int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<Long, List<Long>> loadMappedGenes(int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getUniqueReadCount(int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public TranscriptMappingResults getTranscriptMapping(int index, int key, long experimentId, int distance, Map<Long, List<Long>> mappedTranscripts) {

        TranscriptMappingResults tmr = new TranscriptMappingResults();
        List<Long> mt = mappedTranscripts.get(experimentId);

        Experimentkey ekey = EXPERIMENTKEYMAPPER.get(experimentId, distance);
        BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", experimentId).setInt("d", distance);
        ResultSet _rs = SESSION.execute(bs);
        Experiment experiment = EXPERIMENTMAPPER.map(_rs).one();

        bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", distance);
        _rs = SESSION.execute(bs);
        Datastatistics ds = DATASTATISTICSMAPPER.map(_rs).one();

        Strand strand;

        if (experiment != null && experiment.getAnnotation() != null) {

            if (experiment.getAnnotation().contains("directional")) {
                if (experiment.getAnnotation().endsWith("_FORWARD")) {
                    strand = Strand.FORWARD;
                } else {
                    strand = Strand.REVERSECOMPLEMENT;
                }
            } else {
                strand = Strand.BOTH;
            }

            if (mt != null && mt.size() > index) {
                try {
                    Long transcriptId = mt.get(index);
                    tmr.transcriptID = transcriptId;
                    bs = TRANSCRIPTQUERY.bind().setLong("ti", transcriptId);
                    _rs = SESSION.execute(bs);
                    Transcript transcript = TRANSCRIPTMAPPER.map(_rs).one();
                    int tl = transcript.getSequence().length();
                    Transcriptalignmentsummary tals = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(experimentId, distance, tmr.transcriptID);

                    if (tals != null) {
                        tmr.mappedCount = tals.getMappedreads();
                        tmr.totalMappedCount = tals.getTotalmappedreads();
                        tmr.transcriptLength = tl;
                        double transcriptLength = (new Integer(tl)).doubleValue() / (double) 1000;
                        double totalMappedReads = (ds.getTotalmappedreads().doubleValue() / (double) 1000000);
                        tmr.rpkm = ((new Long(tmr.totalMappedCount)).doubleValue() / transcriptLength) / totalMappedReads;
                    } else {
                        bs = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", experimentId).setInt("d", distance).setLong("ti", transcriptId);
                        _rs = SESSION.execute(bs);
                        Experimentsummary es = EXPERIMENTSUMMARYMAPPER.map(_rs).one();
                        if (es == null) {
                            long[] _counts = CreateExperimentSummaries.getMappedReadCounts(experimentId, distance, tmr.transcriptID, strand);
                            tmr.mappedCount = _counts[0];
                            tmr.totalMappedCount = _counts[1];
                            switch (strand) {
                                case FORWARD:
                                    long fc = ds.getTotalmappedreads() - ds.getTotalmappedreadsRC();
                                    tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / fc;
                                    break;
                                case REVERSECOMPLEMENT:
                                    tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreadsRC();
                                    break;
                                case BOTH:
                                    tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                                    break;
                                default:
                                    tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                                    break;
                            }
                        } else {
                            tmr.mappedCount = es.getNonredundantreads();
                            tmr.totalMappedCount = es.getTotalreads();
                            tmr.rpkm = es.getRpkm();
                        }

                        tmr.transcriptLength = tl;

                        Transcriptalignmentsummary tas = new Transcriptalignmentsummary();
                        tas.setExperimentid(experimentId);
                        tas.setTranscriptid(tmr.transcriptID);
                        tas.setAlignment(null);
                        tas.setMappedreads(tmr.mappedCount);
                        tas.setTotalmappedreads(tmr.totalMappedCount);
                        tas.setMultimappers(null);
                        tas.setCoverages(null);
                        TRANSCRIPTALIGNMENTSUMMARYMAPPER.save(tas);
                    }
                    tmr.name = transcript.getName();
                    tmr.transcriptID = transcript.getTranscriptid();
                    tmr.symbol = transcript.getName();

                    bs = BIOTYPESQUERY.bind().setLong("ti", experimentId);
                    _rs = SESSION.execute(bs);
                    List<Biotypes> biotypes = BIOTYPESMAPPER.map(_rs).all();

                    if (biotypes == null || biotypes.isEmpty()) {
                        tmr.biotype = "Unavailable";
                    } else {
                        Biotypes bt = (Biotypes) biotypes.get(0);
                        String biotype = bt.getBiotype();
                        tmr.biotype = biotype;
                        tmr.synonymousNames = bt.getSynonymousnames();
                        tmr.levelIclassification = bt.getLeveliclassification();
                        tmr.levelIIclassification = bt.getLeveliiclassification();
                        tmr.levelIIIclassification = bt.getLeveliiiclassification();
                        tmr.levelIVclassification = bt.getLevelivclassification();
                        tmr.hierarchyup = bt.getHierarchyup();
                        tmr.hierarchydown = bt.getHierarchydown();
                        tmr.hierarchy0isoform = bt.getHierarchy0isoform();
                        tmr.hierarchy0mutation = bt.getHierarchy0mutation();
                        tmr.hierarchy0other = bt.getHierarchy0other();
                        tmr.levelIfunction = bt.getLevelifunction();
                        tmr.levelIgenestructure = bt.getLeveligenestructure();
                        tmr.disease = bt.getDisease();
                        tmr.roleincancers = bt.getRoleincancers();
                    }
                    return tmr;
                } catch (ConstraintViolationException cve) {
                    Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, cve);
                } catch (Exception e) {
                    Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return tmr;
    }

    @Override
    public List<TranscriptMappingResults> getTranscriptMappingByName(String transcriptName, int key, long experimentId, int distance) {

        List<TranscriptMappingResults> results = new ArrayList<>();
        TranscriptMappingResults tmr = new TranscriptMappingResults();

        Experimentkey ekey = EXPERIMENTKEYMAPPER.get(experimentId, distance);
        BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", experimentId).setInt("d", distance);
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

        try {
            SearchResponse response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("name", transcriptName)).get();
            SearchHit hit = response.getHits().getAt(0);
            Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
            String name = hit.field("name").getValue().toString();
            Transcript transcript = TRANSCRIPTMAPPER.get(1, tid, name);

            int tl = transcript.getSequence().length();
            Transcriptalignmentsummary tals = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(experimentId, distance, tid);

            tmr.transcriptID = tid;
            if (tals != null) {
                tmr.mappedCount = tals.getMappedreads();
                tmr.totalMappedCount = tals.getTotalmappedreads();
                tmr.transcriptLength = tl;
                double transcriptLength = (new Integer(tl)).doubleValue() / (double) 1000;
                double totalMappedReads = (ds.getTotalmappedreads().doubleValue() / (double) 1000000);
                tmr.rpkm = ((new Long(tmr.totalMappedCount)).doubleValue() / transcriptLength) / totalMappedReads;
            } else {
                bs = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", experimentId).setInt("d", distance).setLong("ti", transcript.getTranscriptid());
                _rs = SESSION.execute(bs);
                Experimentsummary es = EXPERIMENTSUMMARYMAPPER.map(_rs).one();
                if (es == null) {
                    long[] _counts = CreateExperimentSummaries.getMappedReadCounts(experimentId, distance, tmr.transcriptID, strand);
                    tmr.mappedCount = _counts[0];
                    tmr.totalMappedCount = _counts[1];
                    switch (strand) {
                        case FORWARD:
                            long fc = ds.getTotalmappedreads() - ds.getTotalmappedreadsRC();
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / fc;
                            break;
                        case REVERSECOMPLEMENT:
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreadsRC();
                            break;
                        case BOTH:
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                            break;
                        default:
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                            break;
                    }
                } else {
                    tmr.mappedCount = es.getNonredundantreads();
                    tmr.totalMappedCount = es.getTotalreads();
                    tmr.rpkm = es.getRpkm();
                }

                tmr.transcriptLength = tl;

                Transcriptalignmentsummary tas = new Transcriptalignmentsummary();
                tas.setExperimentid(experimentId);
                tas.setTranscriptid(tmr.transcriptID);
                tas.setAlignment(null);
                tas.setMappedreads(tmr.mappedCount);
                tas.setTotalmappedreads(tmr.totalMappedCount);
                tas.setMultimappers(null);
                tas.setCoverages(null);
                TRANSCRIPTALIGNMENTSUMMARYMAPPER.save(tas);
            }
            tmr.name = transcript.getName();
            tmr.transcriptID = transcript.getTranscriptid();
            tmr.symbol = transcript.getName();

            bs = BIOTYPESQUERY.bind().setLong("ti", transcript.getTranscriptid());
            _rs = SESSION.execute(bs);
            List<Biotypes> biotypes = BIOTYPESMAPPER.map(_rs).all();

            if (biotypes == null || biotypes.isEmpty()) {
                tmr.biotype = "Unavailable";
            } else {
                Biotypes bt = (Biotypes) biotypes.get(0);
                String biotype = bt.getBiotype();
                tmr.biotype = biotype;
                tmr.synonymousNames = bt.getSynonymousnames();
                tmr.levelIclassification = bt.getLeveliclassification();
                tmr.levelIIclassification = bt.getLeveliiclassification();
                tmr.levelIIIclassification = bt.getLeveliiiclassification();
                tmr.levelIVclassification = bt.getLevelivclassification();
                tmr.hierarchyup = bt.getHierarchyup();
                tmr.hierarchydown = bt.getHierarchydown();
                tmr.hierarchy0isoform = bt.getHierarchy0isoform();
                tmr.hierarchy0mutation = bt.getHierarchy0mutation();
                tmr.hierarchy0other = bt.getHierarchy0other();
                tmr.levelIfunction = bt.getLevelifunction();
                tmr.levelIgenestructure = bt.getLeveligenestructure();
                tmr.disease = bt.getDisease();
                tmr.roleincancers = bt.getRoleincancers();
            }
            results.add(tmr);
        } catch (ConstraintViolationException cve) {
            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, cve);
        } catch (Exception e) {
            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, e);
        }
        return results;
    }

    @Override
    public TranscriptMappingResults populateAlignmentDisplay(TranscriptMappingResults tmr, int key, long experimentId, int distance) {
        try {

            Transcriptalignmentsummary tals = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(experimentId, distance, tmr.transcriptID);

            Experimentkey ekey = EXPERIMENTKEYMAPPER.get(experimentId, distance);
            BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", experimentId).setInt("d", distance);
            ResultSet _rs = SESSION.execute(bs);
            Experiment experiment = EXPERIMENTMAPPER.map(_rs).one();

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

            bs = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", experimentId).setInt("d", distance).setLong("ti", tmr.transcriptID);
            _rs = SESSION.execute(bs);
            Experimentsummary es = EXPERIMENTSUMMARYMAPPER.map(_rs).one();

            bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", distance);
            _rs = SESSION.execute(bs);
            Datastatistics ds = DATASTATISTICSMAPPER.map(_rs).one();

            bs = TRANSCRIPTQUERY.bind().setLong("ti", tmr.transcriptID);
            _rs = SESSION.execute(bs);
            Transcript transcript = TRANSCRIPTMAPPER.map(_rs).one();

            if (tals == null || tals.getAlignment() == null) {

                bs = TRANSCRIPTALIGNMENTPOLYAQUERY.bind().setLong("ti", tmr.transcriptID);
                _rs = SESSION.execute(bs);
                List<Transcriptalignmentpolya> polyaAlignments = TRANSCRIPTALIGNMENTPOLYAMAPPER.map(_rs).all();

                bs = TRANSCRIPTALIGNMENTBARTELPOLYAQUERY.bind().setLong("ti", tmr.transcriptID);
                _rs = SESSION.execute(bs);
                List<Transcriptalignmentbartelpolya> bartelPolyaAlignments = TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.map(_rs).all();

                bs = TRANSCRIPTALIGNMENTTSSQUERY.bind().setLong("ti", tmr.transcriptID);
                _rs = SESSION.execute(bs);
                List<Transcriptalignmenttss> tssAlignments = TRANSCRIPTALIGNMENTTSSMAPPER.map(_rs).all();

                switch (distance) {
                    case 0:
                        StackExactMatch sem = new StackExactMatch();
                        bs = TRANSCRIPTALIGNMENTQUERY.bind().setLong("ei", experimentId).setLong("ti", tmr.transcriptID);
                        _rs = SESSION.execute(bs);
                        List<Transcriptalignment> alignments = TRANSCRIPTALIGNMENTMAPPER.map(_rs).all();
                        tmr = sem.formatWithCAGEPolyAAndBartelPolyA(tmr, transcript, alignments, polyaAlignments, tssAlignments, bartelPolyaAlignments);
                        break;
                    case 1:
                        Stack1Mismatch s1m = new Stack1Mismatch();
                        bs = TRANSCRIPTALIGNMENT1MISMATCHQUERY.bind().setLong("ei", experimentId).setLong("ti", tmr.transcriptID);
                        _rs = SESSION.execute(bs);
                        List<Transcriptalignment1mismatch> alignments1 = TRANSCRIPTALIGNMENT1MISMATCHMAPPER.map(_rs).all();
                        tmr = s1m.formatWithCAGEPolyAAndBartelPolyA(tmr, transcript, alignments1, polyaAlignments, tssAlignments, bartelPolyaAlignments);
                        break;
                    case 2:
                        Stack2Mismatch s2m = new Stack2Mismatch();
                        bs = TRANSCRIPTALIGNMENT2MISMATCHQUERY.bind().setLong("ei", experimentId).setLong("ti", tmr.transcriptID);
                        _rs = SESSION.execute(bs);
                        List<Transcriptalignment2mismatch> alignments2 = TRANSCRIPTALIGNMENT2MISMATCHMAPPER.map(_rs).all();
                        tmr = s2m.formatWithCAGEPolyAAndBartelPolyA(tmr, transcript, alignments2, polyaAlignments, tssAlignments, bartelPolyaAlignments);
                        break;
                    default:
                        break;
                }

                int tl = transcript.getSequence().length();
                tmr.transcriptLength = tl;
                if (es == null) {
                    long[] counts = CreateExperimentSummaries.getMappedReadCounts(experimentId, distance, tmr.transcriptID, strand);
                    tmr.mappedCount = counts[0];
                    tmr.totalMappedCount = counts[1];
                    switch (strand) {
                        case FORWARD:
                            long fc = ds.getTotalmappedreads() - ds.getTotalmappedreadsRC();
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / fc;
                            break;
                        case REVERSECOMPLEMENT:
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreadsRC();
                            break;
                        case BOTH:
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                            break;
                        default:
                            tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                            break;
                    }
                } else {
                    tmr.mappedCount = es.getNonredundantreads();
                    tmr.totalMappedCount = es.getTotalreads();
                    tmr.rpkm = es.getRpkm();
                }

                Transcriptalignmentsummary tas = new Transcriptalignmentsummary();
                tas.setExperimentid(experimentId);
                tas.setDistance(distance);
                tas.setTranscriptid(tmr.transcriptID);
                String map = tmr.mappedAlignments;
                byte[] bytes = ZipUtil.compressBytes(map);
                tas.setAlignment(ByteBuffer.wrap(bytes));
                tas.setMappedreads(tmr.mappedCount);
                tas.setTotalmappedreads(tmr.totalMappedCount);

                List multiMappers = new ArrayList();
                multiMappers.add(0, tmr.multiMappers);
                multiMappers.add(1, tmr.rcMultiMappers);
                map = encode(multiMappers);
                bytes = ZipUtil.compressBytes(map);
                tas.setMultimappers(ByteBuffer.wrap(bytes));

                map = encode(tmr.coverages);
                bytes = ZipUtil.compressBytes(map);
                tas.setCoverages(ByteBuffer.wrap(bytes));

                TRANSCRIPTALIGNMENTSUMMARYMAPPER.save(tas);
            } else if (tals.getAlignment() != null) {
                String map = ZipUtil.extractBytes(tals.getAlignment().array());
                tmr.mappedAlignments = map;
                int tl = transcript.getSequence().length();
                tmr.transcriptLength = tl;

                switch (strand) {
                    case FORWARD:
                        long fc = ds.getTotalmappedreads() - ds.getTotalmappedreadsRC();
                        tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / fc;
                        break;
                    case REVERSECOMPLEMENT:
                        tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreadsRC();
                        break;
                    case BOTH:
                        tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                        break;
                    default:
                        tmr.rpkm = 1000000d * (tmr.totalMappedCount / tl) / ds.getTotalmappedreads();
                        break;
                }

                List multiMappers = (List) decode(ZipUtil.extractBytes(tals.getMultimappers().array()));
                tmr.multiMappers = (Map<Integer, List<Long>>) multiMappers.get(0);
                tmr.rcMultiMappers = (Map<Integer, List<Long>>) multiMappers.get(1);
                tmr.coverages = (Map<String, List<Integer>>) decode(ZipUtil.extractBytes(tals.getCoverages().array()));
            }
        } catch (IOException | DataFormatException | ClassNotFoundException ex) {
            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tmr;
    }

    @Override
    public long getTranscriptMappingCount(int key, long experimentId, int distance) {
        Set<Long> mappedTranscripts = new HashSet<>();

        Statement stmt1;
        switch (distance) {
            case 0:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignmenttranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
            case 1:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignment1mismatchtranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
            case 2:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignment2mismatchtranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
            default:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignmenttranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
        }

        ResultSet _rs = SESSION.execute(stmt1);
        while (!_rs.isExhausted()) {
            Row _row = _rs.one();
            mappedTranscripts.add(_row.getLong("transcriptid"));
        }
        return mappedTranscripts.size();
    }

    @Override
    public Long updateAlignment(int transcriptId, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Map<UUID, Set<Long>> loadMappedReads(int key, long experimentId, int distance) {
        Map<UUID, Set<Long>> mappedReads = Collections.synchronizedMap(new HashMap<>());

        Statement stmt1;
        switch (distance) {
            case 0:
                stmt1 = QueryBuilder
                        .select()
                        .column("readid")
                        .column("transcriptid")
                        .from("cache", "transcriptalignmenttranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
            case 1:
                stmt1 = QueryBuilder
                        .select()
                        .column("readid")
                        .column("transcriptid")
                        .from("cache", "transcriptalignment1mismatchtranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
            case 2:
                stmt1 = QueryBuilder
                        .select()
                        .column("readid")
                        .column("transcriptid")
                        .from("cache", "transcriptalignment2mismatchtranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
            default:
                stmt1 = QueryBuilder
                        .select()
                        .column("readid")
                        .column("transcriptid")
                        .from("cache", "transcriptalignmenttranspose")
                        .where(eq("experimentid", experimentId))
                        .setFetchSize(10000);
                break;
        }

        ResultSet _rs = SESSION.execute(stmt1);

        while (!_rs.isExhausted()) {
            Row row = _rs.one();
            UUID rid = row.getUUID("readid");
            Long tid = row.getLong("transcriptid");
            if (!mappedReads.containsKey(rid)) {
                Set<Long> tids = new HashSet<>();
                tids.add(tid);
                mappedReads.put(rid, tids);
            } else {
                mappedReads.get(rid).add(tid);
            }
        }

        return mappedReads;
    }

    @Override
    public int preProcessAlignments(int key, long experimentId, int distance) {
        try {
            Callable callable = (Callable) () -> {
                CreateTranscriptAlignmentSummaries sstp0 = new CreateTranscriptAlignmentSummaries();
                sstp0.runThreads(experimentId, distance);
                return "All alignment summaries in datasets " + experimentId + " are created.";
            };
            CONSUMER_QUEUE.put(callable);
        } catch (InterruptedException ex) {
            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return distance;
    }

    @Override
    public int checkAlignments(int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int createCoverageForExperiment(int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void getTranscriptMappingCountAndAnnotations(int key, long experimentId, int distance, Map<Long, List<Long>> mappedTranscripts, int min, int max, cache.dataimportes.holders.Strand strand) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<TranscriptMappingResults> getMappedTranscripts(int key, long experimentId, int distance, UUID readId) {

        Set<Long> mappedTranscripts = new HashSet<>();

        Statement stmt1;
        switch (distance) {
            case 0:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignmenttranspose")
                        .where(eq("experimentid", experimentId))
                        .and(eq("readid", readId))
                        .setFetchSize(10000);
                break;
            case 1:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignment1mismatchtranspose")
                        .where(eq("experimentid", experimentId))
                        .and(eq("readid", readId))
                        .setFetchSize(10000);
                break;
            case 2:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignment2mismatchtranspose")
                        .where(eq("experimentid", experimentId))
                        .and(eq("readid", readId))
                        .setFetchSize(10000);
                break;
            default:
                stmt1 = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcriptalignmenttranspose")
                        .where(eq("experimentid", experimentId))
                        .and(eq("readid", readId))
                        .setFetchSize(10000);
                break;
        }

        ResultSet _rs = SESSION.execute(stmt1);
        while (!_rs.isExhausted()) {
            Row _row = _rs.one();
            mappedTranscripts.add(_row.getLong("transcriptid"));
        }

        List<TranscriptMappingResults> list = new ArrayList<>();
        try {
            for (Long tid : mappedTranscripts) {
                TranscriptMappingResults tmr = new TranscriptMappingResults();
                BoundStatement bs = TRANSCRIPTQUERY.bind().setLong("ti", tid);
                _rs = SESSION.execute(bs);
                Transcript transcript = TRANSCRIPTMAPPER.map(_rs).one();

                tmr.name = transcript.getName();
                tmr.transcriptID = tid;
                tmr.symbol = tmr.name;

                bs = BIOTYPESQUERY.bind().setLong("ti", tid);
                _rs = SESSION.execute(bs);
                List<Biotypes> biotypes = BIOTYPESMAPPER.map(_rs).all();
                if (biotypes == null || biotypes.isEmpty()) {
                    tmr.biotype = "Unavailable";
                } else {
                    Biotypes bt = (Biotypes) biotypes.get(0);
                    String biotype = bt.getBiotype();
                    tmr.biotype = biotype;
                    tmr.synonymousNames = bt.getSynonymousnames();
                    tmr.levelIclassification = bt.getLeveliclassification();
                    tmr.levelIIclassification = bt.getLeveliiclassification();
                    tmr.levelIIIclassification = bt.getLeveliiclassification();
                    tmr.levelIVclassification = bt.getLevelivclassification();
                    tmr.hierarchyup = bt.getHierarchyup();
                    tmr.hierarchydown = bt.getHierarchydown();
                    tmr.hierarchy0isoform = bt.getHierarchy0isoform();
                    tmr.hierarchy0mutation = bt.getHierarchy0mutation();
                    tmr.hierarchy0other = bt.getHierarchy0other();
                    tmr.levelIfunction = bt.getLevelifunction();
                    tmr.levelIgenestructure = bt.getLeveligenestructure();
                    tmr.disease = bt.getDisease();
                    tmr.roleincancers = bt.getRoleincancers();
                }
                list.add(tmr);
            }
        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return list;
    }

    @Override
    public void collateUniqueMappingsForGene(int key, long experimentId, int distance) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public TranscriptMappingResults previewEditTranscript(TranscriptMappingResults tmr, int key, long experimentId, int distance, String authToken) {
        TranscriptMappingResults tmr1 = null;
        try {
            BoundStatement bs = USERSQUERY.bind().setString("us", "admin");
            Users user = USERSMAPPER.map(SESSION.execute(bs)).one();

            if (user.getPassword().equals(authToken)) {
                Experimentkey ekey = EXPERIMENTKEYMAPPER.get(experimentId, distance);
                bs = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", experimentId).setInt("d", distance);
                final Experiment e = EXPERIMENTMAPPER.map(SESSION.execute(bs)).one();
                String mp = e.getReadlength();
                int sl = 101;
                if (mp.equals("101") && distance == 0) {
                    sl = 101;
                } else if (mp.equals("101") && (distance == 1 || distance == 2)) {
                    sl = 40;
                } else if (mp.equals("97") && distance == 0) {
                    sl = 97;
                } else if (mp.equals("97") && (distance == 1 || distance == 2)) {
                    sl = 40;
                } else if (mp.equals("75") && distance == 0) {
                    sl = 75;
                } else if (mp.equals("75") && (distance == 1 || distance == 2)) {
                    sl = 40;
                } else if (mp.equals("65") && distance == 0) {
                    sl = 65;
                } else if (mp.equals("65") && (distance == 1 || distance == 2)) {
                    sl = 40;
                } else if (mp.equals("50") && distance == 0) {
                    sl = 50;
                } else if (mp.equals("50") && (distance == 1 || distance == 2)) {
                    sl = 40;
                }

                final int sl1 = sl;

                ExecutorService ex = Executors.newFixedThreadPool(4, (Runnable r) -> {
                    Thread t = new Thread(r);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });

                Callable<List<SearchResult>> cr0 = () -> {
                    List<String> tokens = tokenize(tmr.mappedAlignments, Integer.parseInt(e.getReadlength()));
                    ExecutorService _ex = Executors.newFixedThreadPool(10, (Runnable r) -> {
                        Thread t = new Thread(r);
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    });
                    List<Callable<Object>> callables = new ArrayList<>();

                    Set<SearchResult> srs = Collections.synchronizedSet(new HashSet<>());

                    tokens.stream().forEach((_item) -> {
                        Callable<Object> callable = () -> {
                            List<SearchResult> _srs = searchReadsExactMatchScrolling(e.getId(), _item, Integer.parseInt(e.getReadlength()), SearchResult.SearchType.READS, Strand.BOTH);
                            srs.addAll(_srs);
                            return srs;
                        };
                        callables.add(callable);
                    });

                    _ex.invokeAll(callables);
                    _ex.shutdown();

                    List<SearchResult> sr1 = new ArrayList<>();
                    sr1.addAll(srs);
                    return sr1;
                };

//                Callable<List<SearchResult>> cr0 = () -> DataAccessES.searchReadsExactMatchScrolling(experimentId, tmr.mappedAlignments, sl1, SearchResult.SearchType.READS, Strand.BOTH);
                Callable<List<SearchResult>> cr1 = () -> DataAccessES.searchPolyaReads(tmr.mappedAlignments, 20, SearchResult.SearchType.READS, Strand.BOTH);
                Callable<List<SearchResult>> cr2 = () -> DataAccessES.searchBartelPolyaReads(tmr.mappedAlignments, 20, SearchResult.SearchType.READS, Strand.BOTH);
                Callable<List<SearchResult>> cr3 = () -> DataAccessES.searchTssReads(tmr.mappedAlignments, 20, SearchResult.SearchType.READS, Strand.BOTH);

                Future<List<SearchResult>> f0 = ex.submit(cr0);
                Future<List<SearchResult>> f1 = ex.submit(cr1);
                Future<List<SearchResult>> f2 = ex.submit(cr2);
                Future<List<SearchResult>> f3 = ex.submit(cr3);

                List<SearchResult> results = f0.get();
                List<SearchResult> polyaResults = f1.get();
                List<SearchResult> bartelPolyaResults = f2.get();
                List<SearchResult> tssResults = f3.get();

                ex.shutdown();

                StackExactMatch stack = new StackExactMatch();
                tmr1 = stack.formatWithSearchResults(tmr, results, polyaResults, bartelPolyaResults, tssResults);
            }
        } catch (InterruptedException | ExecutionException ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
        return tmr1;
    }

    @Override
    public void persistEditTranscript(TranscriptMappingResults tmr, List<AnnotationResults> arList, int key, long experimentId, int distance, boolean all, String authToken, String email) {
        try {

            BoundStatement bs = USERSQUERY.bind().setString("us", "admin");
            Users user = USERSMAPPER.map(SESSION.execute(bs)).one();
            long oldTid = tmr.transcriptID;

            if (user.getPassword().equals(authToken)) {
                // Insert Transcript into transcript table
                if (tmr.mappedAlignments == null) {
                    return;
                }
                final String transcript = tmr.mappedAlignments.toUpperCase();
                final Transcript t1 = new Transcript();
                String name1 = tmr.name;

                SearchResponse response = TRANSCRIPTSEARCHBUILDER.setQuery(QueryBuilders.matchQuery("name", tmr.name.toLowerCase())).get();

                if (response.getHits().getHits().length > 0) {
                    if (name1.length() > 60) {
                        name1 = name1.substring(0, 60);
                    }
                    name1 += "_" + System.currentTimeMillis();
                    tmr.name = name1;
                }

                t1.setKey(1);
                t1.setname(name1);
                t1.setSequence(transcript);
                bs = MAXTRANSCRIPTIDQUERY.bind();
                Long maxTranscriptId = SESSION.execute(bs).one().getLong(0);
                t1.setTranscriptid(maxTranscriptId + 1);
                TRANSCRIPTMAPPER.save(t1);

                DataAccessES.indexTranscript(t1.getTranscriptid(), t1.getName(), t1.getSequence());

                //Insert into Biotypes table
                BoundStatement bs1 = BIOTYPESQUERY.bind().setLong("ti", tmr.transcriptID);
                List<Biotypes> biotypes = BIOTYPESMAPPER.map(SESSION.execute(bs1)).all();
                biotypes.stream().map((b) -> {
                    Biotypes _b = new Biotypes();
                    _b.setKey(1);
                    _b.setTranscriptid(t1.getTranscriptid());
                    _b.setSymbol(t1.getName());
                    _b.setBiotype(b.getBiotype());
                    _b.setDisease(b.getDisease());
                    _b.setHierarchydown(b.getHierarchydown());
                    _b.setHierarchyup(b.getHierarchyup());
                    _b.setHierarchy0isoform(b.getHierarchy0isoform());
                    _b.setHierarchy0mutation(b.getHierarchy0mutation());
                    _b.setHierarchy0other(b.getHierarchy0other());
                    _b.setLevelivclassification(b.getLevelivclassification());
                    _b.setLeveliiiclassification(b.getLeveliiiclassification());
                    _b.setLeveliiclassification(b.getLeveliiclassification());
                    _b.setLeveliclassification(b.getLeveliclassification());
                    _b.setLevelifunction(b.getLevelifunction());
                    _b.setLeveligenestructure(b.getLeveligenestructure());
                    _b.setRoleincancers(b.getRoleincancers());
                    _b.setSynonymousnames(b.getSynonymousnames());
                    DataAccessES.editTranscriptSynonyms(t1.getTranscriptid(), b.getSynonymousnames());
                    return _b;
                }).forEach((_b) -> {
                    BIOTYPESMAPPER.save(_b);
                });
                tmr.transcriptID = t1.getTranscriptid();
                arList.stream().map((ar) -> {
                    Transcriptannotations ta = new Transcriptannotations();
                    ta.setKey(1);
                    ta.setId(UUIDs.timeBased());
                    ta.setTranscriptid(t1.getTranscriptid());
                    ta.setAnnotation(ar.annotation);
                    ta.setSequence(ar.sequence);
                    ta.setVariant(ar.variant);
                    ta.setStartcoordinate(ar.startCoordinate);
                    ta.setStopcoordinate(ar.stopCoordinate);
                    ta.setReversecomplement(ar.strand.equals(Strand.REVERSECOMPLEMENT));
                    ta.setPredicted(ar.predicted);
                    return ta;
                }).forEach((ta) -> {
                    TRANSCRIPTANNOTATIONSMAPPER.save(ta);
                });

                final String name = name1;

                Callable runnable = (Callable) () -> {
                    try {
                        //Persist PolyA
                        ExecutorService ex = Executors.newFixedThreadPool(3, (Runnable r) -> {
                            Thread t = new Thread(r);
                            t.setPriority(Thread.MAX_PRIORITY);
                            return t;
                        });
                        Callable<List<SearchResult>> cr1 = () -> searchPolyaReads(t1.getSequence(), 20, SearchResult.SearchType.READS, Strand.BOTH);
                        Callable<List<SearchResult>> cr2 = () -> searchBartelPolyaReads(t1.getSequence(), 20, SearchResult.SearchType.READS, Strand.BOTH);
                        Callable<List<SearchResult>> cr3 = () -> searchTssReads(t1.getSequence(), 20, SearchResult.SearchType.READS, Strand.BOTH);
                        Future<List<SearchResult>> f1 = ex.submit(cr1);
                        Future<List<SearchResult>> f2 = ex.submit(cr2);
                        Future<List<SearchResult>> f3 = ex.submit(cr3);
                        List<SearchResult> polyaResults = f1.get();
                        List<SearchResult> bartelPolyaResults = f2.get();
                        List<SearchResult> tssResults = f3.get();
                        ex.shutdown();
                        if (polyaResults != null) {
                            BatchStatement batch = new BatchStatement();
                            for (SearchResult sr : polyaResults) {
                                String rs = sr.sequence;
                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                                    DNASequence sequence = new DNASequence(sr.sequence);
                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                            = new ReversedSequenceView<>(
                                                    new ComplementSequenceView<>(sequence));
                                    rs = revComp.getSequenceAsString();
                                }
                                String es = t1.getSequence();
                                int sc = StringUtils.indexOf(es, rs);
                                int cm = StringUtils.countMatches(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
                                    taPolya.setKey(1);
                                    taPolya.setReadid(sr.readID);
                                    taPolya.setId(UUIDs.timeBased());
                                    taPolya.setCount(sr.readCount);
                                    taPolya.setTranscriptid(tmr.transcriptID);
                                    taPolya.setStartcoordinate(sc);
                                    taPolya.setStopcoordinate((long) (sc + rs.length()));
                                    taPolya.setScore((double) rs.length());
                                    taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, t1.getTranscriptid(), sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
                                    batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                                if (cm > 1) {
                                    while (sc >= 0) {
                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                        sc = StringUtils.indexOf(es, rs);
                                        if (sc >= 0) {
                                            Transcriptalignmentpolya taPolya = new Transcriptalignmentpolya();
                                            taPolya.setKey(1);
                                            taPolya.setReadid(sr.readID);
                                            taPolya.setId(UUIDs.timeBased());
                                            taPolya.setCount(sr.readCount);
                                            taPolya.setTranscriptid(tmr.transcriptID);
                                            taPolya.setStartcoordinate(sc);
                                            taPolya.setStopcoordinate((long) (sc + rs.length()));
                                            taPolya.setScore((double) rs.length());
                                            taPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                            Transcriptalignmentpolyatranspose tat = new Transcriptalignmentpolyatranspose(1, t1.getTranscriptid(), sr.readID);
                                            batch.add(TRANSCRIPTALIGNMENTPOLYAMAPPER.saveQuery(taPolya));
                                            batch.add(TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                            if (batch.size() > 100) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            if (batch.size() > 0) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        //Persist Bartel PolyA
                        if (bartelPolyaResults != null) {
                            BatchStatement batch = new BatchStatement();
                            for (SearchResult sr : bartelPolyaResults) {
                                String rs = sr.sequence;
                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                                    DNASequence sequence = new DNASequence(sr.sequence);
                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                            = new ReversedSequenceView<>(
                                                    new ComplementSequenceView<>(sequence));
                                    rs = revComp.getSequenceAsString();
                                }
                                String es = t1.getSequence();
                                int sc = StringUtils.indexOf(es, rs);
                                int cm = StringUtils.countMatches(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
                                    taBPolya.setKey(1);
                                    taBPolya.setReadid(sr.readID);
                                    taBPolya.setId(UUIDs.timeBased());
                                    taBPolya.setCount(sr.readCount);
                                    taBPolya.setTranscriptid(tmr.transcriptID);
                                    taBPolya.setStartcoordinate(sc);
                                    taBPolya.setStopcoordinate((long) (sc + rs.length()));
                                    taBPolya.setScore((double) rs.length());
                                    taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, t1.getTranscriptid(), sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
                                    batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                                if (cm > 1) {
                                    while (sc >= 0) {
                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                        sc = StringUtils.indexOf(es, rs);
                                        if (sc >= 0) {
                                            Transcriptalignmentbartelpolya taBPolya = new Transcriptalignmentbartelpolya();
                                            taBPolya.setKey(1);
                                            taBPolya.setReadid(sr.readID);
                                            taBPolya.setId(UUIDs.timeBased());
                                            taBPolya.setCount(sr.readCount);
                                            taBPolya.setTranscriptid(tmr.transcriptID);
                                            taBPolya.setStartcoordinate(sc);
                                            taBPolya.setStopcoordinate((long) (sc + rs.length()));
                                            taBPolya.setScore((double) rs.length());
                                            taBPolya.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                            Transcriptalignmentbartelpolyatranspose tat = new Transcriptalignmentbartelpolyatranspose(1, t1.getTranscriptid(), sr.readID);
                                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.saveQuery(taBPolya));
                                            batch.add(TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER.saveQuery(tat));
                                            if (batch.size() > 100) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            if (batch.size() > 0) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        //Persist TSS
                        if (tssResults != null) {
                            BatchStatement batch = new BatchStatement();
                            for (SearchResult sr : tssResults) {
                                String rs = sr.sequence;
                                if (sr.strand.equals(Strand.REVERSECOMPLEMENT)) {
                                    DNASequence sequence = new DNASequence(sr.sequence);
                                    org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                                            = new ReversedSequenceView<>(
                                                    new ComplementSequenceView<>(sequence));
                                    rs = revComp.getSequenceAsString();
                                }
                                String es = t1.getSequence();
                                int sc = StringUtils.indexOf(es, rs);
                                int cm = StringUtils.countMatches(es, rs);
                                if (sc >= 0) {
                                    Transcriptalignmenttss taTss = new Transcriptalignmenttss();
                                    taTss.setKey(1);
                                    taTss.setReadid(sr.readID);
                                    taTss.setId(UUIDs.timeBased());
                                    taTss.setCount(sr.readCount);
                                    taTss.setTranscriptid(tmr.transcriptID);
                                    taTss.setStartcoordinate(sc);
                                    taTss.setStopcoordinate((long) (sc + rs.length()));
                                    taTss.setScore((double) rs.length());
                                    taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                    Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, t1.getTranscriptid(), sr.readID);
                                    batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
                                    batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
                                    if (batch.size() > 100) {
                                        SESSION.execute(batch);
                                        batch.clear();
                                    }
                                }
                                if (cm > 1) {
                                    while (sc >= 0) {
                                        es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                                        sc = StringUtils.indexOf(es, rs);
                                        if (sc >= 0) {
                                            Transcriptalignmenttss taTss = new Transcriptalignmenttss();
                                            taTss.setKey(1);
                                            taTss.setReadid(sr.readID);
                                            taTss.setId(UUIDs.timeBased());
                                            taTss.setCount(sr.readCount);
                                            taTss.setTranscriptid(tmr.transcriptID);
                                            taTss.setStartcoordinate(sc);
                                            taTss.setStopcoordinate((long) (sc + rs.length()));
                                            taTss.setScore((double) rs.length());
                                            taTss.setReversecomplement(sr.strand.equals(Strand.REVERSECOMPLEMENT));
                                            Transcriptalignmenttsstranspose tat = new Transcriptalignmenttsstranspose(1, t1.getTranscriptid(), sr.readID);
                                            batch.add(TRANSCRIPTALIGNMENTTSSMAPPER.saveQuery(taTss));
                                            batch.add(TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER.saveQuery(tat));
                                            if (batch.size() > 100) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                                if (batch.size() > 100) {
                                    SESSION.execute(batch);
                                    batch.clear();
                                }
                            }
                            if (batch.size() > 0) {
                                SESSION.execute(batch);
                                batch.clear();
                            }
                        }
                        if (all) {
                            if (t1.getSequence().length() >= 30000) {
                                ex = Executors.newFixedThreadPool(35, (Runnable r) -> {
                                    Thread t = new Thread(r);
                                    t.setPriority(Thread.MAX_PRIORITY);
                                    return t;
                                });
                            } else {
                                ex = Executors.newFixedThreadPool(65, (Runnable r) -> {
                                    Thread t = new Thread(r);
                                    t.setPriority(Thread.MAX_PRIORITY);
                                    return t;
                                });
                            }
                            // Persist Alignments
                            List<Callable<Object>> callables = new ArrayList<>();
                            Set<Experiment> experiments = new HashSet<>();
                            for (int k = 0; k < 100; k++) {
                                Statement stmt1 = QueryBuilder
                                        .select()
                                        .column("id")
                                        .column("distance")
                                        .from("cache", "experiment")
                                        .where(eq("key", k));
                                ResultSet _rs = SESSION.execute(stmt1);
                                while (!_rs.isExhausted()) {
                                    Row row = _rs.one();
                                    int distance1 = row.getInt("distance");
                                    if (distance1 == 0) {
                                        Long eid = row.getLong("id");
                                        BoundStatement _bs1 = EXPERIMENTQUERY.bind().setInt("ky", k).setLong("id", eid).setInt("d", 0);
                                        experiments.add(EXPERIMENTMAPPER.map(SESSION.execute(_bs1)).one());
                                    }
                                }
                            }

                            experiments.stream().map((e) -> new CallableImplPrimaryAlignments(e, t1, tmr)).forEach((c) -> {
                                callables.add(c);
                            });
                            ex.invokeAll(callables);
                            ex.shutdown();

                        } else {
                            ex = Executors.newFixedThreadPool(1, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                            List<Callable<Object>> callables = new ArrayList<>();
                            Experimentkey ekey = EXPERIMENTKEYMAPPER.get(experimentId, 0);
                            BoundStatement _bs1 = EXPERIMENTQUERY.bind().setInt("ky", ekey.getKey()).setLong("id", experimentId).setInt("d", 0);
                            final Experiment e = EXPERIMENTMAPPER.map(SESSION.execute(_bs1)).one();
                            Callable c = new CallableImplPrimaryAlignmentsParallel(e, t1, tmr);
                            callables.add(c);
                            ex.invokeAll(callables);
                            ex.shutdown();
                        }
                        /**
                         * Send email
                         */
                        String subject = "Your edit transcript process of " + tmr.name;
                        String to = email;
                        String message = "The process of persisting the edited transcript, " + tmr.name + ", is complete. "
                                + "The new transcript is available from searches across all the experiments";
                        Email.sendMessage(to, subject, message);
                    } catch (InterruptedException | ExecutionException | CompoundNotFoundException ex) {
                        Logger.getLogger(AlignmentServiceImpl.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                    return Boolean.TRUE;
                };

                if (CONSUMER_QUEUE.size() > 0) {
                    /**
                     * Send email
                     */
                    String subject = "Your edit transcript process of " + tmr.name + " is queued.";
                    String to = email;
                    String message = "The process of persisting the edited transcript, " + tmr.name + ", is queued behind " + (CONSUMER_QUEUE.size() + 1) + " jobs.";
                    Email.sendMessage(to, subject, message);
                }

                CONSUMER_QUEUE.put(runnable);
            }
        } catch (Exception e) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, e);
        }
    }

    protected void persist(long experimentId, Transcript e, String read, UUID rid, Long readCount, boolean rc, BatchStatement batch) {
        String rs = read;
        if (rc) {
            try {
                DNASequence sequence = new DNASequence(read);
                org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> revComp
                        = new ReversedSequenceView<>(
                                new ComplementSequenceView<>(sequence));
                rs = revComp.getSequenceAsString();
            } catch (CompoundNotFoundException ex) {
                Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        String es = e.getSequence();
        int sc = StringUtils.indexOf(es, rs);
        int cm = StringUtils.countMatches(es, rs);
        if (sc >= 0) {
            Transcriptalignment taTss = new Transcriptalignment();
            taTss.setExperimentid(experimentId);
            taTss.setReadid(rid);
            taTss.setId(UUIDs.timeBased());
            taTss.setReadcount(readCount);
            taTss.setTranscriptid(e.getTranscriptid());
            taTss.setStartcoordinate(sc);
            taTss.setStopcoordinate((long) (sc + rs.length()));
            taTss.setScore((double) rs.length());
            taTss.setReversecomplement(rc);
            Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, e.getTranscriptid());
            if (tat == null) {
                tat = new Transcriptalignmenttranspose(experimentId, e.getTranscriptid(), rid, 1);
            } else {
                int _c = tat.getCount();
                _c++;
                tat.setCount(_c);
            }
            batch.add(TRANSCRIPTALIGNMENTMAPPER.saveQuery(taTss));
            batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.saveQuery(tat));
            batch.add(UNMAPPEDREADSMAPPER.deleteQuery(experimentId, rid));
        }
        if (cm > 1) {
            while (sc >= 0) {
                es = StringUtils.replaceFirst(es, rs, StringUtils.repeat("X", rs.length()));
                sc = StringUtils.indexOf(es, rs);
                if (sc >= 0) {
                    Transcriptalignment taTss = new Transcriptalignment();
                    taTss.setExperimentid(experimentId);
                    taTss.setReadid(rid);
                    taTss.setId(UUIDs.timeBased());
                    taTss.setReadcount(readCount);
                    taTss.setTranscriptid(e.getTranscriptid());
                    taTss.setStartcoordinate(sc);
                    taTss.setStopcoordinate((long) (sc + rs.length()));
                    taTss.setScore((double) rs.length());
                    taTss.setReversecomplement(rc);
                    Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, e.getTranscriptid());
                    if (tat == null) {
                        tat = new Transcriptalignmenttranspose(experimentId, e.getTranscriptid(), rid, 1);
                    } else {
                        int _c = tat.getCount();
                        _c++;
                        tat.setCount(_c);
                    }
                    batch.add(TRANSCRIPTALIGNMENTMAPPER.saveQuery(taTss));
                    batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.saveQuery(tat));
                    batch.add(UNMAPPEDREADSMAPPER.deleteQuery(experimentId, rid));
                }
            }
        }
    }

    protected void persistExperimentSummary(TranscriptMappingResults tmr, Experiment experiment, BatchStatement batch, long[] delta) {
        try {
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

            BoundStatement bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experiment.getId()).setInt("d", experiment.getDistance());
            Datastatistics ds = DATASTATISTICSMAPPER.map(SESSION.execute(bs)).one();

            bs = TRANSCRIPTQUERY.bind().setLong("ti", tmr.transcriptID);
            Transcript transcript = TRANSCRIPTMAPPER.map(SESSION.execute(bs)).one();

            int tl = transcript.getSequence().length();

            long[] counts = CreateExperimentSummaries.getMappedReadCountsAndTranscriptsMapped(experiment.getId(), experiment.getDistance(), tmr.transcriptID, strand);
            tmr.mappedCount = counts[0];
            tmr.totalMappedCount = counts[1];
            double mappedReadsBylength = 1000d * (double) counts[1] / (double) tl;
            switch (strand) {
                case FORWARD:
                    long fc = (ds.getTotalmappedreads() + delta[0]) - (ds.getTotalmappedreadsRC() + delta[2]);
                    tmr.rpkm = 1000000d * (mappedReadsBylength) / fc;
                    break;
                case REVERSECOMPLEMENT:
                    tmr.rpkm = 1000000d * (mappedReadsBylength) / (ds.getTotalmappedreadsRC() + delta[2]);
                    break;
                case BOTH:
                    tmr.rpkm = 1000000d * (mappedReadsBylength) / (ds.getTotalmappedreads() + delta[0]);
                    break;
                default:
                    tmr.rpkm = 1000000d * (mappedReadsBylength) / (ds.getTotalmappedreads() + delta[0]);
                    break;
            }

            ds.setTotalmappedreads(ds.getTotalmappedreads() + delta[0]);
            ds.setTotalmappedreadsnonredundant(ds.getTotalmappedreadsnonredundant() + delta[1]);
            ds.setTotalmappedreadsRC(ds.getTotalmappedreadsRC() + delta[2]);
            ds.setTotalmappedreadsnonredundantRC(ds.getTotalmappedreadsnonredundantRC() + delta[3]);
            batch.add(DATASTATISTICSMAPPER.saveQuery(ds));

            Experimentsummary summary = new Experimentsummary();
            summary.setExperimentid(experiment.getId());
            summary.setDistance(experiment.getDistance());
            summary.setTranscriptid(transcript.getTranscriptid());
            summary.setId(UUIDs.timeBased());
            summary.setSymbol(transcript.getName());
            summary.setFiltered(Boolean.FALSE);
            summary.setTranscriptlength(tl);
            summary.setNonredundantreads(counts[0]);
            summary.setTotalreads(counts[1]);
            summary.setTranscriptsmappedbyreads(counts[2]);
            summary.setMappedreadsbylength(mappedReadsBylength);
            summary.setRpkm(tmr.rpkm);
            batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(summary));
            SESSION.execute(batch);
            batch.clear();

        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    protected void persistExperimentSummary(Long tid, Experiment experiment, BatchStatement batch) {
        try {
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

            BoundStatement bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experiment.getId()).setInt("d", experiment.getDistance());
            Datastatistics ds = DATASTATISTICSMAPPER.map(SESSION.execute(bs)).one();

            bs = TRANSCRIPTQUERY.bind().setLong("ti", tid);
            Transcript transcript = TRANSCRIPTMAPPER.map(SESSION.execute(bs)).one();

            int tl = transcript.getSequence().length();

            long[] counts = CreateExperimentSummaries.getMappedReadCountsAndTranscriptsMapped(experiment.getId(), experiment.getDistance(), tid, strand);
            double mappedReadsBylength = 1000d * (double) counts[1] / (double) tl;
            double rpkm = 0d;
            switch (strand) {
                case FORWARD:
                    long fc = (ds.getTotalmappedreads()) - (ds.getTotalmappedreadsRC());
                    rpkm = 1000000d * (mappedReadsBylength) / fc;
                    break;
                case REVERSECOMPLEMENT:
                    rpkm = 1000000d * (mappedReadsBylength) / (ds.getTotalmappedreadsRC());
                    break;
                case BOTH:
                    rpkm = 1000000d * (mappedReadsBylength) / (ds.getTotalmappedreads());
                    break;
                default:
                    rpkm = 1000000d * (mappedReadsBylength) / (ds.getTotalmappedreads());
                    break;
            }

            Experimentsummary summary = new Experimentsummary();
            summary.setExperimentid(experiment.getId());
            summary.setDistance(experiment.getDistance());
            summary.setTranscriptid(transcript.getTranscriptid());
            summary.setId(UUIDs.timeBased());
            summary.setSymbol(transcript.getName());
            summary.setFiltered(Boolean.FALSE);
            summary.setTranscriptlength(tl);
            summary.setNonredundantreads(counts[0]);
            summary.setTotalreads(counts[1]);
            summary.setTranscriptsmappedbyreads(counts[2]);
            summary.setMappedreadsbylength(mappedReadsBylength);
            summary.setRpkm(rpkm);
            batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(summary));
            SESSION.execute(batch);
            batch.clear();
        } catch (Exception ex) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, ex);
        }
    }

    protected void updateMultimappersAndDisplay(TranscriptMappingResults tmr, Experiment e, BatchStatement batch) {
        long transcriptId = tmr.transcriptID;
        long eid = e.getId();
        populateAlignmentDisplay(tmr, e.getKey(), e.getId(), e.getDistance());
        Set<Long> multimappers = new HashSet<>();
        Transcriptalignmentsummary tas;
        switch (e.getDistance()) {
            case 0:
                tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 0, transcriptId);
                break;
            case 1:
                tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 1, transcriptId);
                break;
            case 2:
                tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 2, transcriptId);
                break;
            default:
                tas = TRANSCRIPTALIGNMENTSUMMARYMAPPER.get(eid, 0, transcriptId);
                break;
        }
        multimappers.add(transcriptId);
        if (tas.getMultimappers() != null) {
            try {
                List mm = (List) decode(ZipUtil.extractBytes(tas.getMultimappers().array()));
                Map<Integer, List<Long>> multiMappers = (Map<Integer, List<Long>>) mm.get(0);
                Map<Integer, List<Long>> rcMultiMappers = (Map<Integer, List<Long>>) mm.get(1);
                multiMappers.keySet().stream().map((key) -> multiMappers.get(key)).forEach((_tis) -> {
                    _tis.stream().forEach((_t) -> {
                        multimappers.add(_t);
                    });
                });
                rcMultiMappers.keySet().stream().map((key) -> rcMultiMappers.get(key)).forEach((_tis) -> {
                    _tis.stream().forEach((_t) -> {
                        multimappers.add(_t);
                    });
                });
            } catch (Exception ex) {
                Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        multimappers.stream().forEach((tid) -> {
            switch (e.getDistance()) {
                case 0:
                    batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(eid, 0, tid));
                    break;
                case 1:
                    batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(eid, 1, tid));
                    break;
                case 2:
                    batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(eid, 2, tid));
                    break;
                default:
                    batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.deleteQuery(eid, 0, tid));
                    break;
            }
        });
        SESSION.execute(batch);
        batch.clear();
//        multimappers.stream().forEach((tid) -> {
//            TranscriptMappingResults tmr1 = new TranscriptMappingResults();
//            tmr1.transcriptID = tid;
//            populateAlignmentDisplay(tmr1, e.getId(), e.getDistance());
//        });
    }

    @Override
    public Long runAlignmentWithMismatches(List<Long> transcripts) {
        try {

            for (Long tid : transcripts) {

                Callable runnable = (Callable) () -> {
                    try {
                        Statement stmt1 = QueryBuilder
                                .select()
                                .all()
                                .from("cache", "transcript")
                                .where(eq("key", 1))
                                .and(eq("transcriptid", tid));
                        Row row = SESSION.execute(stmt1).one();

                        String name = row.getString("name");
                        String sequence = row.getString("sequence");

                        Transcript t1 = TRANSCRIPTMAPPER.get(1, tid, name);

                        ExecutorService ex;

                        if (t1.getSequence().length() >= 30000) {
                            ex = Executors.newFixedThreadPool(35, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                        } else {
                            ex = Executors.newFixedThreadPool(65, (Runnable r) -> {
                                Thread t = new Thread(r);
                                t.setPriority(Thread.MAX_PRIORITY);
                                return t;
                            });
                        }
                        // Persist Alignments
                        List<Callable<Object>> callables = new ArrayList<>();
                        Set<Experiment> experiments = new HashSet<>();
                        for (int k = 0; k < 100; k++) {
                            Statement stmt2 = QueryBuilder
                                    .select()
                                    .column("id")
                                    .column("distance")
                                    .from("cache", "experiment")
                                    .where(eq("key", k));
                            ResultSet _rs = SESSION.execute(stmt2);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                int distance1 = _row.getInt("distance");
                                if (distance1 == 0) {
                                    Long eid = _row.getLong("id");
                                    BoundStatement bs1 = EXPERIMENTQUERY.bind().setInt("ky", k).setLong("id", eid).setInt("d", 0);
                                    experiments.add(EXPERIMENTMAPPER.map(SESSION.execute(bs1)).one());
                                }
                            }
                        }
                        TranscriptMappingResults tmr = new TranscriptMappingResults();
                        tmr.transcriptID = t1.getTranscriptid();
                        tmr.name = t1.getName();
                        tmr.mappedAlignments = t1.getSequence();
                        experiments.stream().map((e) -> new CallableImplSecondaryAlignments(e, t1, tmr)).forEach((c) -> {
                            callables.add(c);
                        });
                        ex.invokeAll(callables);
                        ex.shutdown();

                        /**
                         * Send email
                         */
                        String subject = "Your alignment transcript process of " + name;
                        String to = "";
                        String message = "The process of aligning transcript, " + name + ", with mismatches, is complete. "
                                + "The new transcript is available from searches across all the experiments";
                        Email.sendMessage(to, subject, message);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(AlignmentServiceImpl.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                    return Boolean.TRUE;
                };
                CONSUMER_QUEUE.put(runnable);
            }
        } catch (Exception e) {
            Logger.getLogger(AlignmentServiceImpl.class
                    .getName()).log(Level.SEVERE, null, e);
        }
        return 0l;
    }

    class CallableImplDeleteAlignments implements Callable {

        Long eid;
        Long transcriptId;

        public CallableImplDeleteAlignments(Long eid, Long transcriptId) {
            this.eid = eid;
            this.transcriptId = transcriptId;
        }

        @Override
        public Object call() throws Exception {
            System.out.println("Experimentid, transcriptid: " + eid + ", " + transcriptId);
            try {
                BatchStatement batch = new BatchStatement();
                Set<UUID> deletedReads = new HashSet<>();
                BoundStatement _bs1 = TRANSCRIPTALIGNMENTQUERY.bind().setLong("ei", eid).setLong("ti", transcriptId);
                List<Transcriptalignment> tal = TRANSCRIPTALIGNMENTMAPPER.map(SESSION.execute(_bs1)).all();
                tal.stream().map((ta) -> {
                    deletedReads.add(ta.getReadid());
                    return ta;
                }).map((ta) -> {
                    batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.deleteQuery(eid, ta.getReadid(), transcriptId));
                    return ta;
                }).map((ta) -> {
                    batch.add(TRANSCRIPTALIGNMENTMAPPER.deleteQuery(ta));
                    return ta;
                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                    SESSION.execute(batch);
                    return _item;
                }).forEach((_item) -> {
                    batch.clear();
                });
                _bs1 = TRANSCRIPTALIGNMENT1MISMATCHQUERY.bind().setLong("ei", eid).setLong("ti", transcriptId);
                List<Transcriptalignment1mismatch> tal1 = TRANSCRIPTALIGNMENT1MISMATCHMAPPER.map(SESSION.execute(_bs1)).all();
                tal1.stream().map((ta) -> {
                    deletedReads.add(ta.getReadid());
                    return ta;
                }).map((ta) -> {
                    batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.deleteQuery(eid, ta.getReadid(), transcriptId));
                    return ta;
                }).map((ta) -> {
                    batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.deleteQuery(ta));
                    return ta;
                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                    SESSION.execute(batch);
                    return _item;
                }).forEach((_item) -> {
                    batch.clear();
                });
                _bs1 = TRANSCRIPTALIGNMENT2MISMATCHQUERY.bind().setLong("ei", eid).setLong("ti", transcriptId);
                List<Transcriptalignment2mismatch> tal2 = TRANSCRIPTALIGNMENT2MISMATCHMAPPER.map(SESSION.execute(_bs1)).all();
                tal2.stream().map((ta) -> {
                    deletedReads.add(ta.getReadid());
                    return ta;
                }).map((ta) -> {
                    batch.add(TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.deleteQuery(eid, ta.getReadid(), transcriptId));
                    return ta;
                }).map((ta) -> {
                    batch.add(TRANSCRIPTALIGNMENT2MISMATCHMAPPER.deleteQuery(ta));
                    return ta;
                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                    SESSION.execute(batch);
                    return _item;
                }).forEach((_item) -> {
                    batch.clear();
                });
                SESSION.execute(batch);
                batch.clear();
                for (UUID rid : deletedReads) {
                    _bs1 = TRANSCRIPTALIGNMENTTRANSPOSEQUERY.bind().setLong("ei", eid).setUUID("ri", rid);
                    List<Transcriptalignmenttranspose> talt = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.map(SESSION.execute(_bs1)).all();
                    _bs1 = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEQUERY.bind().setLong("ei", eid).setUUID("ri", rid);
                    List<Transcriptalignment1mismatchtranspose> tal1t = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.map(SESSION.execute(_bs1)).all();
                    _bs1 = TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEQUERY.bind().setLong("ei", eid).setUUID("ri", rid);
                    List<Transcriptalignment2mismatchtranspose> tal2t = TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.map(SESSION.execute(_bs1)).all();
                    if (talt.isEmpty() && tal1t.isEmpty() && tal2t.isEmpty()) {
                        Unmappedreads umr = new Unmappedreads(eid, rid);
                        batch.add(UNMAPPEDREADSMAPPER.saveQuery(umr));
                    }
                    if (batch.size() > 100) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                }

                if (batch.size()
                        > 0) {
                    SESSION.execute(batch);
                    batch.clear();
                }
            } catch (Exception ex) {
                Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
            return Boolean.TRUE;
        }
    }

    class CallableImplPrimaryAlignmentsParallel implements Callable {

        Experiment e;
        Transcript t1;
        TranscriptMappingResults tmr;
        String transcript;
        List<String> tokens;

        public CallableImplPrimaryAlignmentsParallel(Experiment e, Transcript t1, TranscriptMappingResults tmr) {
            this.e = e;
            this.t1 = t1;
            this.tmr = tmr;
            this.transcript = t1.getSequence();
            tokens = tokenize(this.transcript, Integer.parseInt(e.getReadlength()));
        }

        @Override
        public Object call() throws Exception {
            try {
                System.out.println("Invoked callable: " + e.getId());
                BatchStatement batch = new BatchStatement();

                ExecutorService ex = Executors.newFixedThreadPool(10, (Runnable r) -> {
                    Thread t = new Thread(r);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });
                List<Callable<Object>> callables = new ArrayList<>();

                Set<SearchResult> srs = Collections.synchronizedSet(new HashSet<>());

                tokens.stream().forEach((_item) -> {
                    Callable<Object> callable = () -> {
                        List<SearchResult> _srs = searchReadsExactMatchScrolling(e.getId(), _item, Integer.parseInt(e.getReadlength()), SearchResult.SearchType.READS, Strand.BOTH);
                        srs.addAll(_srs);
                        return srs;
                    };
                    callables.add(callable);
                });

                ex.invokeAll(callables);
                ex.shutdown();

                srs.stream().map((sr) -> {
                    persist(e.getId(), t1, sr.sequence, sr.readID, sr.readCount, sr.strand.equals(Strand.REVERSECOMPLEMENT), batch);
                    return sr;
                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                    SESSION.execute(batch);
                    return _item;
                }).forEach((_item) -> {
                    batch.clear();
                });
                SESSION.execute(batch);
                batch.clear();
                persistExperimentSummary(tmr.transcriptID, e, batch);
                updateMultimappersAndDisplay(tmr, e, batch);

                if (batch.size() > 0) {
                    SESSION.execute(batch);
                    batch.clear();
                }
            } catch (Exception exception) {
                Logger.getLogger(AlignmentServiceImpl.class
                        .getName()).log(Level.SEVERE, null, exception);
            }
            System.out.println("Callable primary alignments done: " + e.getId());
            return Boolean.TRUE;
        }
    }

    class CallableImplPrimaryAlignments implements Callable {

        Experiment e;
        Transcript t1;
        TranscriptMappingResults tmr;
        String transcript;
        List<String> tokens;

        public CallableImplPrimaryAlignments(Experiment e, Transcript t1, TranscriptMappingResults tmr) {
            this.e = e;
            this.t1 = t1;
            this.tmr = tmr;
            this.transcript = t1.getSequence();
            tokens = tokenize(this.transcript, Integer.parseInt(e.getReadlength()));
        }

        @Override
        public Object call() throws Exception {
            try {
                BoundStatement _bs1 = EXPERIMENTSUMMARYQUERY.bind().setLong("eid", e.getId()).setInt("d", 0).setLong("ti", t1.getTranscriptid());

                Experimentsummary es = EXPERIMENTSUMMARYMAPPER.map(SESSION.execute(_bs1)).one();
                if (es == null) {
                    System.out.println("Invoked callable: " + e.getId());
                    BatchStatement batch = new BatchStatement();

                    Set<SearchResult> srs = new HashSet<>();
                    tokens.stream().map((_item) -> searchReadsExactMatchScrolling(e.getId(), _item, Integer.parseInt(e.getReadlength()), SearchResult.SearchType.READS, Strand.BOTH)).forEach((_srs) -> {
                        srs.addAll(_srs);
                    });
                    srs.stream().map((sr) -> {
                        persist(e.getId(), t1, sr.sequence, sr.readID, sr.readCount, sr.strand.equals(Strand.REVERSECOMPLEMENT), batch);
                        return sr;
                    }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                        SESSION.execute(batch);
                        return _item;
                    }).forEach((_item) -> {
                        batch.clear();
                    });
                    SESSION.execute(batch);
                    batch.clear();
                    persistExperimentSummary(tmr.transcriptID, e, batch);
                    updateMultimappersAndDisplay(tmr, e, batch);

                    if (batch.size() > 0) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                }
            } catch (Exception exception) {
                Logger.getLogger(AlignmentServiceImpl.class
                        .getName()).log(Level.SEVERE, null, exception);
            }
            System.out.println("Callable primary alignments done: " + e.getId());
            return Boolean.TRUE;
        }
    }

    class CallableImplAlignments implements Callable {

        Experiment e;
        Transcript t1;
        TranscriptMappingResults tmr;
        String transcript;
        Long experimentId;

        public CallableImplAlignments(Experiment e, Transcript t1, TranscriptMappingResults tmr) {
            this.e = e;
            this.t1 = t1;
            this.tmr = tmr;
            this.transcript = t1.getSequence();
            experimentId = e.getId();
        }

        @Override
        public Object call() {
            try {
                final Object lock = new Object();
                System.out.println("Invoked callable: " + e.getId());
                BatchStatement batch = new BatchStatement();
                List<SearchResult> srs = searchReadsAllMatchesFuzzyScrolling(e.getId(), transcript, Integer.parseInt(e.getReadlength()), SearchResult.SearchType.READS, Strand.BOTH);
                System.out.println("Matches : " + srs.size() + ", in experiment: " + e.getId());
                srs.stream().map((sr) -> {
                    try {
                        String _s = sr.sequence;
                        DNASequence _sequence = new DNASequence(_s);
                        org.biojava.nbio.core.sequence.template.Sequence rc
                                = new ReversedSequenceView<>(
                                        new ComplementSequenceView<>(_sequence));
                        String revComp = rc.getSequenceAsString();
                        boolean mapped = false;

                        int sc = transcript.indexOf(_s);
                        int cm = StringUtils.countMatches(transcript, _s);
                        if (sc >= 0) {
                            Transcriptalignment ta = new Transcriptalignment(experimentId, t1.getTranscriptid(), UUIDs.timeBased(), sr.readID, false,
                                    sc, sc + _s.length(), sr.readCount, ((Integer) _s.length()).doubleValue());
                            Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, sr.readID, t1.getTranscriptid());
                            if (tat == null) {
                                tat = new Transcriptalignmenttranspose(experimentId, t1.getTranscriptid(), sr.readID, 1);
                            } else {
                                int _c = tat.getCount();
                                _c++;
                                tat.setCount(_c);
                            }
                            synchronized (lock) {
                                batch.add(TRANSCRIPTALIGNMENTMAPPER.saveQuery(ta));
                                batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.saveQuery(tat));
                            }
                            mapped = true;
                            if (cm > 1) {
                                String masked = transcript;
                                while (sc >= 0) {
                                    masked = StringUtils.replaceFirst(masked, _s, StringUtils.repeat("X", _s.length()));
                                    sc = StringUtils.indexOf(masked, _s);
                                    if (sc >= 0) {
                                        ta = new Transcriptalignment(experimentId, t1.getTranscriptid(), UUIDs.timeBased(), sr.readID, false,
                                                sc, sc + _s.length(), sr.readCount, ((Integer) _s.length()).doubleValue());
                                        tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, sr.readID, t1.getTranscriptid());
                                        if (tat == null) {
                                            tat = new Transcriptalignmenttranspose(experimentId, t1.getTranscriptid(), sr.readID, 1);
                                        } else {
                                            int _c = tat.getCount();
                                            _c++;
                                            tat.setCount(_c);
                                        }
                                        synchronized (lock) {
                                            batch.add(TRANSCRIPTALIGNMENTMAPPER.saveQuery(ta));
                                            batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.saveQuery(tat));
                                        }
                                    }
                                }
                            }
                        } else {
                            Fuzzy fuzzy = new Fuzzy();
                            double distance = fuzzy.containability(transcript, _s) * _s.length();
                            if (Math.rint(distance) == 1) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), _s);
                                    if (ledr.getDistance() == 1.0) {
                                        persist1DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, _s, sr.readCount, false, batch);
                                        mapped = true;
                                    } else if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, _s, sr.readCount, false, batch);
                                        mapped = true;
                                    }
                                }
                            } else if (Math.rint(distance) == 2) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), _s);
                                    if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, _s, sr.readCount, false, batch);
                                        mapped = true;
                                    }
                                }
                            }
                        }

                        sc = transcript.indexOf(revComp);
                        cm = StringUtils.countMatches(transcript, revComp);
                        if (sc >= 0) {
                            Transcriptalignment ta = new Transcriptalignment(experimentId, t1.getTranscriptid(), UUIDs.timeBased(), sr.readID, true,
                                    sc, sc + revComp.length(), sr.readCount, ((Integer) revComp.length()).doubleValue());
                            Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, sr.readID, t1.getTranscriptid());
                            if (tat == null) {
                                tat = new Transcriptalignmenttranspose(experimentId, t1.getTranscriptid(), sr.readID, 1);
                            } else {
                                int _c = tat.getCount();
                                _c++;
                                tat.setCount(_c);
                            }
                            synchronized (lock) {
                                batch.add(TRANSCRIPTALIGNMENTMAPPER.saveQuery(ta));
                                batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.saveQuery(tat));
                            }
                            mapped = true;
                            if (cm > 1) {
                                String masked = transcript;
                                while (sc >= 0) {
                                    masked = StringUtils.replaceFirst(masked, revComp, StringUtils.repeat("X", revComp.length()));
                                    sc = StringUtils.indexOf(masked, revComp);
                                    if (sc >= 0) {
                                        ta = new Transcriptalignment(experimentId, t1.getTranscriptid(), UUIDs.timeBased(), sr.readID, true,
                                                sc, sc + revComp.length(), sr.readCount, ((Integer) revComp.length()).doubleValue());
                                        tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, sr.readID, t1.getTranscriptid());
                                        if (tat == null) {
                                            tat = new Transcriptalignmenttranspose(experimentId, t1.getTranscriptid(), sr.readID, 1);
                                        } else {
                                            int _c = tat.getCount();
                                            _c++;
                                            tat.setCount(_c);
                                        }
                                        synchronized (lock) {
                                            batch.add(TRANSCRIPTALIGNMENTMAPPER.saveQuery(ta));
                                            batch.add(TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.saveQuery(tat));
                                        }
                                    }
                                }
                            }
                        } else {
                            Fuzzy fuzzy = new Fuzzy();
                            double distance = fuzzy.containability(transcript, revComp) * revComp.length();
                            if (Math.rint(distance) == 1) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), revComp);
                                    if (ledr.getDistance() == 1.0) {
                                        persist1DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, revComp, sr.readCount, true, batch);
                                        mapped = true;
                                    } else if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, revComp, sr.readCount, true, batch);
                                        mapped = true;
                                    }
                                }
                            } else if (Math.rint(distance) == 2) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), revComp);
                                    if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, revComp, sr.readCount, true, batch);
                                        mapped = true;
                                    }
                                }
                            }
                        }
                        if (mapped) {
                            batch.add(UNMAPPEDREADSMAPPER.deleteQuery(new Unmappedreads(e.getId(), sr.readID)));
                        }
                    } catch (CompoundNotFoundException ex) {
                        Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    return sr;
                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                    SESSION.execute(batch);
                    return _item;
                }).forEach((_item) -> {
                    batch.clear();
                });
                SESSION.execute(batch);
                batch.clear();
                persistExperimentSummary(tmr.transcriptID, e, batch);
                updateMultimappersAndDisplay(tmr, e, batch);
                e.setDistance(1);
                persistExperimentSummary(tmr.transcriptID, e, batch);
                updateMultimappersAndDisplay(tmr, e, batch);
                e.setDistance(2);
                persistExperimentSummary(tmr.transcriptID, e, batch);
                updateMultimappersAndDisplay(tmr, e, batch);
                if (batch.size() > 0) {
                    SESSION.execute(batch);
                    batch.clear();
                }
            } catch (Exception exception) {
                Logger.getLogger(AlignmentServiceImpl.class
                        .getName()).log(Level.SEVERE, null, exception);
            }
            System.out.println("Callable alignments done: " + e.getId());
            return Boolean.TRUE;
        }

        void persist1DL(DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr, String e, int start, Long tid, Long eid, UUID rid, String r, Long readCount, boolean rc, BatchStatement batch) {
            String editLine = ledr.getEditSequence();
            String eString = ledr.getTopAlignmentRow();
            String rString = ledr.getBottomAlignmentRow();

            Integer m1c = 0;
            String mt = "";
            String v = "";
            if (editLine.contains("S")) {
                m1c = editLine.indexOf("S");
                if (rc) {
                    m1c = r.length() - 1 - m1c;
                }
                mt = "S";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("TT")) {
                m1c = editLine.indexOf("TT");
                mt = "TT";
                v = r.substring(m1c, m1c + 2);
            } else if (editLine.contains("I")) {
                m1c = eString.indexOf("-");
                mt = "I";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("D")) {
                m1c = rString.indexOf("-");
                mt = "D";
                v = e.substring(m1c, m1c + 1);
            }

            if (!v.equals("")) {
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + r.length(), readCount,
                        ledr.getDistance(), m1c, mt, v);
                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist2DL(DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr, String e, int start, Long tid, Long eid, UUID rid, String r, Long readCount, boolean rc, BatchStatement batch) {
            Integer mc1 = 0, mc2 = 0;
            String mt1 = "", mt2 = "";
            String v1 = "", v2 = "";

            String editLine = ledr.getEditSequence();
            String eString = ledr.getTopAlignmentRow();
            String rString = ledr.getBottomAlignmentRow();
            if (editLine.endsWith("DNI")) {
                editLine = StringUtils.replace(editLine, "DNI", "SS");
                eString = StringUtils.replace(eString, "-", "");
                rString = StringUtils.replace(rString, "-", "");
            } else if (editLine.endsWith("IND")) {
                editLine = StringUtils.replace(editLine, "IND", "SS");
                eString = StringUtils.replace(eString, "-", "");
                rString = StringUtils.replace(rString, "-", "");
            }
            if (StringUtils.countMatches(editLine, "S") == 2) {
                mc1 = editLine.indexOf("S");
                mt1 = "S";
                if (rc) {
                    mc1 = r.length() - 1 - mc1;
                }
                v1 = r.substring(mc1, mc1 + 1);
                editLine = StringUtils.replaceFirst(editLine, "S", StringUtils.repeat("X", 1));
                mc2 = StringUtils.indexOf(editLine, "S");
                mt2 = "S";
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
                v2 = r.substring(mc2, mc2 + 1);
            } else if (StringUtils.countMatches(editLine, "TT") == 2) {
                mc1 = editLine.indexOf("TT");
                mt1 = "TT";
                v1 = r.substring(mc1, mc1 + 2);
                editLine = StringUtils.replaceFirst(editLine, "TT", StringUtils.repeat("X", 2));
                mc2 = StringUtils.indexOf(editLine, "TT");
                mt2 = "TT";
                v2 = r.substring(mc2, mc2 + 2);
            } else if (StringUtils.countMatches(editLine, "I") == 2) {
                int iIndex = eString.indexOf("-");
                mc1 = iIndex;
                v1 = rString.substring(iIndex, iIndex + 1);
                mt1 = "I";
                eString = StringUtils.replaceFirst(eString, "-", StringUtils.repeat("X", 1));
                iIndex = eString.indexOf("-");
                mc2 = iIndex;
                v2 = rString.substring(iIndex, iIndex + 1);
                mt2 = "I";
            } else if (StringUtils.countMatches(editLine, "D") == 2) {
                int dIndex = rString.indexOf("-");
                mc1 = dIndex;
                v1 = eString.substring(dIndex, dIndex + 1);
                mt1 = "D";
                rString = StringUtils.replaceFirst(rString, "-", StringUtils.repeat("X", 1));
                dIndex = rString.indexOf("-");
                mc2 = dIndex;
                v2 = eString.substring(dIndex, dIndex + 1);
                mt2 = "D";
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "TT") == 1) {
                mc1 = editLine.indexOf("S");
                mt1 = "S";
                if (rc) {
                    mc1 = r.length() - 1 - mc1;
                }
                v1 = r.substring(mc1, mc1 + 1);
                mc2 = editLine.indexOf("TT");
                mt2 = "TT";
                v2 = r.substring(mc2, mc2 + 2);
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                mc1 = eString.indexOf("-");
                v1 = rString.substring(mc1, mc1 + 1);
                mt1 = "I";
                mc2 = editLine.indexOf("S");
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
                v2 = rString.substring(mc2, mc2 + 1);
                mt2 = "S";
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                mc1 = rString.indexOf("-");
                v1 = eString.substring(mc1, mc1 + 1);
                mt1 = "D";
                mc2 = editLine.indexOf("S");
                v2 = rString.substring(mc2, mc2 + 1);
                if (mc2 > mc1) {
                    mc2--;
                }
                mt2 = "S";
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
            } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                mc1 = eString.indexOf("-");
                v1 = rString.substring(mc1, mc1 + 1);
                mt1 = "I";
                mc2 = editLine.indexOf("TT");
                v2 = rString.substring(mc2, mc2 + 2);
                mt2 = "TT";
            } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                mc1 = rString.indexOf("-");
                v1 = eString.substring(mc1, mc1 + 1);
                mt1 = "D";
                mc2 = editLine.indexOf("TT");
                v2 = rString.substring(mc2, mc2 + 2);
                if (mc2 > mc1) {
                    mc2--;
                }
                mt2 = "TT";
            } else if (StringUtils.countMatches(editLine, "I") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                int iIndex = eString.indexOf("-");
                v1 = rString.substring(iIndex, iIndex + 1);
                mt1 = "I";
                int dIndex = rString.indexOf("-");
                v2 = eString.substring(dIndex, dIndex + 1);
                mt2 = "D";
                if (iIndex > dIndex) {
                    mc1 = iIndex - 1;
                    mc2 = dIndex;
                } else {
                    mc1 = iIndex;
                    mc2 = dIndex;
                }
            }

            if (!(v1.equals("") && v2.equals(""))) {
                Transcriptalignment2mismatch ta = new Transcriptalignment2mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + r.length(), readCount,
                        ledr.getDistance(), mc1, mt1, v1, mc2, mt2, v2);

                Transcriptalignment2mismatchtranspose tat = TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment2mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT2MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist1S(Sequence es, Long tid, Long eid, UUID rid, Sequence rs, Long readCount, Integer length, boolean rc, BatchStatement batch) {

            if (es != null && rs != null && identity != null) {
                Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 10, 10);
                float score = readAlignment.getScore();
                int start = readAlignment.getStart1();
                if (readAlignment.getStart2() > 0) {
                    start -= readAlignment.getStart2();
                }
                String ref = es.getSequence().substring(start, start + rs.getSequence().length());
                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                        = DamerauLevenshteinEditDistance.compute(ref, rs.getSequence());
                String editLine = ledr.getEditSequence();

                Integer m1c = 0;
                String mt = "";
                String v = "";
                if (editLine.contains("S")) {
                    m1c = editLine.indexOf("S");
                    mt = "S";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                } else if (editLine.contains("TT")) {
                    m1c = editLine.indexOf("TT");
                    mt = "TT";
                    v = rs.getSequence().substring(m1c, m1c + 2);
                }
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + rs.getSequence().length(), readCount,
                        ledr.getDistance(), m1c, mt, v);

                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist1ID(Sequence es, Long tid, Long eid, UUID rid, Sequence rs, Long readCount, Integer length, boolean rc, BatchStatement batch) {

            if (es != null && rs != null && identity != null) {
                Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 0.5f, 10);
                float score = readAlignment.getScore();
                int start = readAlignment.getStart1();
                if (readAlignment.getStart2() > 0) {
                    start -= readAlignment.getStart2();
                }
                String ref = es.getSequence().substring(start, start + rs.getSequence().length());
                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                        = DamerauLevenshteinEditDistance.compute(ref, rs.getSequence());
                String editLine = ledr.getEditSequence();

                Integer m1c = 0;
                String mt = "";
                String v = "";
                int dIndex = 0;
                int iIndex = 0;
                if (editLine.contains("I")) {
                    iIndex = editLine.indexOf("I");
                }

                if (editLine.contains("D")) {
                    dIndex = editLine.indexOf("D");
                }

                if (dIndex < iIndex) {
                    m1c = dIndex;
                    mt = "D";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                } else if (iIndex < dIndex) {
                    m1c = iIndex;
                    mt = "I";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                }
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + rs.getSequence().length(), readCount,
                        ledr.getDistance(), m1c, mt, v);

                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }
    }

    class CallableImplSecondaryAlignments implements Callable {

        Experiment e;
        Transcript t1;
        TranscriptMappingResults tmr;
        String transcript;
        Long experimentId;

        public CallableImplSecondaryAlignments(Experiment e, Transcript t1, TranscriptMappingResults tmr) {
            this.e = e;
            this.t1 = t1;
            this.tmr = tmr;
            this.transcript = t1.getSequence();
            experimentId = e.getId();
        }

        @Override
        public Object call() {
            try {
                System.out.println("Invoked callable: " + e.getId());
                BatchStatement batch = new BatchStatement();
                List<SearchResult> srs = searchReadsAllMatchesFuzzyScrolling2(e.getId(), transcript, Integer.parseInt(e.getReadlength()), SearchResult.SearchType.READS, Strand.BOTH);
                System.out.println("Matches : " + srs.size() + ", in experiment: " + e.getId());
                srs.stream().map((sr) -> {
                    if (TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, sr.readID, t1.getTranscriptid()) == null) {
                        try {
                            String _s = sr.sequence;
                            DNASequence _sequence = new DNASequence(_s);
                            org.biojava.nbio.core.sequence.template.Sequence rc
                                    = new ReversedSequenceView<>(
                                            new ComplementSequenceView<>(_sequence));
                            String revComp = rc.getSequenceAsString();
                            boolean mapped = false;
                            Fuzzy fuzzy = new Fuzzy();
                            double distance = fuzzy.containability(transcript, _s) * _s.length();
                            if (Math.rint(distance) == 1) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), _s);
                                    if (ledr.getDistance() == 1.0) {
                                        persist1DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, _s, sr.readCount, false, batch);
                                        mapped = true;
                                    } else if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, _s, sr.readCount, false, batch);
                                        mapped = true;
                                    }
                                }
                            } else if (Math.rint(distance) == 2) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), _s);
                                    if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, _s, sr.readCount, false, batch);
                                        mapped = true;
                                    }
                                }
                            }
                            fuzzy = new Fuzzy();
                            distance = fuzzy.containability(transcript, revComp) * revComp.length();
                            if (Math.rint(distance) == 1) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), revComp);
                                    if (ledr.getDistance() == 1.0) {
                                        persist1DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, revComp, sr.readCount, true, batch);
                                        mapped = true;
                                    } else if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, revComp, sr.readCount, true, batch);
                                        mapped = true;
                                    }
                                }
                            } else if (Math.rint(distance) == 2) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= transcript.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(transcript.substring(st, end), revComp);
                                    if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, transcript.substring(st, end), st, t1.getTranscriptid(), e.getId(), sr.readID, revComp, sr.readCount, true, batch);
                                        mapped = true;
                                    }
                                }
                            }
                            if (mapped) {
                                batch.add(UNMAPPEDREADSMAPPER.deleteQuery(new Unmappedreads(e.getId(), sr.readID)));
                            }
                        } catch (CompoundNotFoundException ex) {
                            Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    return sr;
                }).filter((_item) -> (batch.size() > 100)).map((_item) -> {
                    SESSION.execute(batch);
                    return _item;
                }).forEach((_item) -> {
                    batch.clear();
                });
                SESSION.execute(batch);
                batch.clear();
                e.setDistance(1);
                persistExperimentSummary(tmr.transcriptID, e, batch);
                updateMultimappersAndDisplay(tmr, e, batch);
                e.setDistance(2);
                persistExperimentSummary(tmr.transcriptID, e, batch);
                updateMultimappersAndDisplay(tmr, e, batch);
                if (batch.size() > 0) {
                    SESSION.execute(batch);
                    batch.clear();
                }
            } catch (Exception exception) {
                Logger.getLogger(AlignmentServiceImpl.class
                        .getName()).log(Level.SEVERE, null, exception);
            }
            System.out.println("Callable secondary alignments done: " + e.getId());
            return Boolean.TRUE;
        }

        void persist1DL(DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr, String e, int start, Long tid, Long eid, UUID rid, String r, Long readCount, boolean rc, BatchStatement batch) {
            String editLine = ledr.getEditSequence();
            String eString = ledr.getTopAlignmentRow();
            String rString = ledr.getBottomAlignmentRow();

            Integer m1c = 0;
            String mt = "";
            String v = "";
            if (editLine.contains("S")) {
                m1c = editLine.indexOf("S");
                if (rc) {
                    m1c = r.length() - 1 - m1c;
                }
                mt = "S";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("TT")) {
                m1c = editLine.indexOf("TT");
                mt = "TT";
                v = r.substring(m1c, m1c + 2);
            } else if (editLine.contains("I")) {
                m1c = eString.indexOf("-");
                mt = "I";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("D")) {
                m1c = rString.indexOf("-");
                mt = "D";
                v = e.substring(m1c, m1c + 1);
            }

            if (!v.equals("")) {
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + r.length(), readCount,
                        ledr.getDistance(), m1c, mt, v);
                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist2DL(DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr, String e, int start, Long tid, Long eid, UUID rid, String r, Long readCount, boolean rc, BatchStatement batch) {
            Integer mc1 = 0, mc2 = 0;
            String mt1 = "", mt2 = "";
            String v1 = "", v2 = "";

            String editLine = ledr.getEditSequence();
            String eString = ledr.getTopAlignmentRow();
            String rString = ledr.getBottomAlignmentRow();
            if (editLine.endsWith("DNI")) {
                editLine = StringUtils.replace(editLine, "DNI", "SS");
                eString = StringUtils.replace(eString, "-", "");
                rString = StringUtils.replace(rString, "-", "");
            } else if (editLine.endsWith("IND")) {
                editLine = StringUtils.replace(editLine, "IND", "SS");
                eString = StringUtils.replace(eString, "-", "");
                rString = StringUtils.replace(rString, "-", "");
            }
            if (StringUtils.countMatches(editLine, "S") == 2) {
                mc1 = editLine.indexOf("S");
                mt1 = "S";
                if (rc) {
                    mc1 = r.length() - 1 - mc1;
                }
                v1 = r.substring(mc1, mc1 + 1);
                editLine = StringUtils.replaceFirst(editLine, "S", StringUtils.repeat("X", 1));
                mc2 = StringUtils.indexOf(editLine, "S");
                mt2 = "S";
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
                v2 = r.substring(mc2, mc2 + 1);
            } else if (StringUtils.countMatches(editLine, "TT") == 2) {
                mc1 = editLine.indexOf("TT");
                mt1 = "TT";
                v1 = r.substring(mc1, mc1 + 2);
                editLine = StringUtils.replaceFirst(editLine, "TT", StringUtils.repeat("X", 2));
                mc2 = StringUtils.indexOf(editLine, "TT");
                mt2 = "TT";
                v2 = r.substring(mc2, mc2 + 2);
            } else if (StringUtils.countMatches(editLine, "I") == 2) {
                int iIndex = eString.indexOf("-");
                mc1 = iIndex;
                v1 = rString.substring(iIndex, iIndex + 1);
                mt1 = "I";
                eString = StringUtils.replaceFirst(eString, "-", StringUtils.repeat("X", 1));
                iIndex = eString.indexOf("-");
                mc2 = iIndex;
                v2 = rString.substring(iIndex, iIndex + 1);
                mt2 = "I";
            } else if (StringUtils.countMatches(editLine, "D") == 2) {
                int dIndex = rString.indexOf("-");
                mc1 = dIndex;
                v1 = eString.substring(dIndex, dIndex + 1);
                mt1 = "D";
                rString = StringUtils.replaceFirst(rString, "-", StringUtils.repeat("X", 1));
                dIndex = rString.indexOf("-");
                mc2 = dIndex;
                v2 = eString.substring(dIndex, dIndex + 1);
                mt2 = "D";
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "TT") == 1) {
                mc1 = editLine.indexOf("S");
                mt1 = "S";
                if (rc) {
                    mc1 = r.length() - 1 - mc1;
                }
                v1 = r.substring(mc1, mc1 + 1);
                mc2 = editLine.indexOf("TT");
                mt2 = "TT";
                v2 = r.substring(mc2, mc2 + 2);
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                mc1 = eString.indexOf("-");
                v1 = rString.substring(mc1, mc1 + 1);
                mt1 = "I";
                mc2 = editLine.indexOf("S");
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
                v2 = rString.substring(mc2, mc2 + 1);
                mt2 = "S";
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                mc1 = rString.indexOf("-");
                v1 = eString.substring(mc1, mc1 + 1);
                mt1 = "D";
                mc2 = editLine.indexOf("S");
                v2 = rString.substring(mc2, mc2 + 1);
                if (mc2 > mc1) {
                    mc2--;
                }
                mt2 = "S";
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
            } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                mc1 = eString.indexOf("-");
                v1 = rString.substring(mc1, mc1 + 1);
                mt1 = "I";
                mc2 = editLine.indexOf("TT");
                v2 = rString.substring(mc2, mc2 + 2);
                mt2 = "TT";
            } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                mc1 = rString.indexOf("-");
                v1 = eString.substring(mc1, mc1 + 1);
                mt1 = "D";
                mc2 = editLine.indexOf("TT");
                v2 = rString.substring(mc2, mc2 + 2);
                if (mc2 > mc1) {
                    mc2--;
                }
                mt2 = "TT";
            } else if (StringUtils.countMatches(editLine, "I") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                int iIndex = eString.indexOf("-");
                v1 = rString.substring(iIndex, iIndex + 1);
                mt1 = "I";
                int dIndex = rString.indexOf("-");
                v2 = eString.substring(dIndex, dIndex + 1);
                mt2 = "D";
                if (iIndex > dIndex) {
                    mc1 = iIndex - 1;
                    mc2 = dIndex;
                } else {
                    mc1 = iIndex;
                    mc2 = dIndex;
                }
            }

            if (!(v1.equals("") && v2.equals(""))) {
                Transcriptalignment2mismatch ta = new Transcriptalignment2mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + r.length(), readCount,
                        ledr.getDistance(), mc1, mt1, v1, mc2, mt2, v2);

                Transcriptalignment2mismatchtranspose tat = TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment2mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT2MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist1S(Sequence es, Long tid, Long eid, UUID rid, Sequence rs, Long readCount, Integer length, boolean rc, BatchStatement batch) {

            if (es != null && rs != null && identity != null) {
                Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 10, 10);
                float score = readAlignment.getScore();
                int start = readAlignment.getStart1();
                if (readAlignment.getStart2() > 0) {
                    start -= readAlignment.getStart2();
                }
                String ref = es.getSequence().substring(start, start + rs.getSequence().length());
                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                        = DamerauLevenshteinEditDistance.compute(ref, rs.getSequence());
                String editLine = ledr.getEditSequence();

                Integer m1c = 0;
                String mt = "";
                String v = "";
                if (editLine.contains("S")) {
                    m1c = editLine.indexOf("S");
                    mt = "S";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                } else if (editLine.contains("TT")) {
                    m1c = editLine.indexOf("TT");
                    mt = "TT";
                    v = rs.getSequence().substring(m1c, m1c + 2);
                }
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + rs.getSequence().length(), readCount,
                        ledr.getDistance(), m1c, mt, v);

                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist1ID(Sequence es, Long tid, Long eid, UUID rid, Sequence rs, Long readCount, Integer length, boolean rc, BatchStatement batch) {

            if (es != null && rs != null && identity != null) {
                Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 0.5f, 10);
                float score = readAlignment.getScore();
                int start = readAlignment.getStart1();
                if (readAlignment.getStart2() > 0) {
                    start -= readAlignment.getStart2();
                }
                String ref = es.getSequence().substring(start, start + rs.getSequence().length());
                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                        = DamerauLevenshteinEditDistance.compute(ref, rs.getSequence());
                String editLine = ledr.getEditSequence();

                Integer m1c = 0;
                String mt = "";
                String v = "";
                int dIndex = 0;
                int iIndex = 0;
                if (editLine.contains("I")) {
                    iIndex = editLine.indexOf("I");
                }

                if (editLine.contains("D")) {
                    dIndex = editLine.indexOf("D");
                }

                if (dIndex < iIndex) {
                    m1c = dIndex;
                    mt = "D";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                } else if (iIndex < dIndex) {
                    m1c = iIndex;
                    mt = "I";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                }
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + rs.getSequence().length(), readCount,
                        ledr.getDistance(), m1c, mt, v);

                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }
    }

    class CallableImplSecondaryAlignments1 implements Callable {

        Long experimentId;
        Long tid;
        String sequence;
        SearchRequestBuilder srb;

        public CallableImplSecondaryAlignments1(long experimentId, long tid, String sequence) {
            this.experimentId = experimentId;
            this.tid = tid;
            this.sequence = sequence;
            srb = CLIENT.prepareSearch("index_transcripts_40")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("transcriptid")
                    .setRouting("1")
                    .setExplain(false);
        }

        @Override
        public Object call() {
            Statement stmt = QueryBuilder
                    .select()
                    .column("readid")
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId))
                    .setFetchSize(100000);

            ResultSet rs = SESSION.execute(stmt);

            int count = 0;

            BatchStatement batch = new BatchStatement();
            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    UUID rid = row.getUUID("readid");

                    BoundStatement bs = READSQUERY.bind().setLong("ei", experimentId).setUUID("ri", rid);
                    Row _r = SESSION.execute(bs).one();

                    String _s = _r.getString("sequence");
                    Long readCount = _r.getLong("count");

                    DNASequence _sequence = new DNASequence(_s);
                    org.biojava.nbio.core.sequence.template.Sequence rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(_sequence));
                    String revComp = rc.getSequenceAsString();

                    boolean mapped = false;

                    SearchResponse response = srb.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("transcriptid", tid)).must(QueryBuilders.termQuery("sequence", _s))).get();
                    if (response.getHits().getHits().length == 1) {
                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(sequence, _s) * _s.length();
                        if (Math.rint(distance) == 1) {
                            int st = fuzzy.getResultStart() - 1;
                            int end = fuzzy.getResultEnd();
                            if (end <= sequence.length()) {
                                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                        = DamerauLevenshteinEditDistance.compute(sequence.substring(st, end), _s);
                                if (ledr.getDistance() == 1.0) {
                                    persist1DL(ledr, sequence.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                    mapped = true;
                                } else if (ledr.getDistance() == 2.0) {
                                    persist2DL(ledr, sequence.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                    mapped = true;
                                }
                            }
                        } else if (Math.rint(distance) == 2) {
                            int st = fuzzy.getResultStart() - 1;
                            int end = fuzzy.getResultEnd();
                            if (end <= sequence.length()) {
                                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                        = DamerauLevenshteinEditDistance.compute(sequence.substring(st, end), _s);
                                if (ledr.getDistance() == 2.0) {
                                    persist2DL(ledr, sequence.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                    mapped = true;
                                }
                            }
                        }
                    }

                    response = srb.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("transcriptid", tid)).must(QueryBuilders.termQuery("sequence", revComp))).get();
                    if (response.getHits().getHits().length == 1) {
                        Fuzzy fuzzy = new Fuzzy();
                        double distance = fuzzy.containability(sequence, revComp) * revComp.length();
                        if (Math.rint(distance) == 1) {
                            int st = fuzzy.getResultStart() - 1;
                            int end = fuzzy.getResultEnd();
                            if (end <= sequence.length()) {
                                String e = sequence.substring(st, end);
                                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                        = DamerauLevenshteinEditDistance.compute(e, revComp);
                                if (ledr.getDistance() == 1.0) {
                                    persist1DL(ledr, e, st, tid, experimentId, rid, revComp, readCount, true, batch);
                                    mapped = true;
                                } else if (ledr.getDistance() == 2.0) {
                                    persist2DL(ledr, e, st, tid, experimentId, rid, revComp, readCount, true, batch);
                                    mapped = true;
                                }
                            }
                        } else if (Math.rint(distance) == 2) {
                            int st = fuzzy.getResultStart() - 1;
                            int end = fuzzy.getResultEnd();
                            if (end <= sequence.length()) {
                                String e = sequence.substring(st, end);
                                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                        = DamerauLevenshteinEditDistance.compute(e, revComp);
                                if (ledr.getDistance() == 2.0) {
                                    persist2DL(ledr, e, st, tid, experimentId, rid, revComp, readCount, true, batch);
                                    mapped = true;
                                }
                            }
                        }
                    }

                    if (mapped) {
                        batch.add(UNMAPPEDREADSMAPPER.deleteQuery(new Unmappedreads(experimentId, rid)));
                    }

                    if (batch.size() > 3000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                } catch (Exception ex) {
                    Logger.getLogger(AlignmentServiceImpl.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (count % 50000 == 0) {
                    System.out.println("AlignmentServiceImpl: Aligned " + count + " reads from experiment "
                            + experimentId);
                }
                count++;
            }

            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }

            System.out.println("Callable secondary alignments done: " + experimentId);
            return Boolean.TRUE;
        }

        void persist1DL(DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr, String e, int start, Long tid, Long eid, UUID rid, String r, Long readCount, boolean rc, BatchStatement batch) {
            String editLine = ledr.getEditSequence();
            String eString = ledr.getTopAlignmentRow();
            String rString = ledr.getBottomAlignmentRow();

            Integer m1c = 0;
            String mt = "";
            String v = "";
            if (editLine.contains("S")) {
                m1c = editLine.indexOf("S");
                if (rc) {
                    m1c = r.length() - 1 - m1c;
                }
                mt = "S";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("TT")) {
                m1c = editLine.indexOf("TT");
                mt = "TT";
                v = r.substring(m1c, m1c + 2);
            } else if (editLine.contains("I")) {
                m1c = eString.indexOf("-");
                mt = "I";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("D")) {
                m1c = rString.indexOf("-");
                mt = "D";
                v = e.substring(m1c, m1c + 1);
            }

            if (!v.equals("")) {
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + r.length(), readCount,
                        ledr.getDistance(), m1c, mt, v);
                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist2DL(DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr, String e, int start, Long tid, Long eid, UUID rid, String r, Long readCount, boolean rc, BatchStatement batch) {
            Integer mc1 = 0, mc2 = 0;
            String mt1 = "", mt2 = "";
            String v1 = "", v2 = "";

            String editLine = ledr.getEditSequence();
            String eString = ledr.getTopAlignmentRow();
            String rString = ledr.getBottomAlignmentRow();
            if (editLine.endsWith("DNI")) {
                editLine = StringUtils.replace(editLine, "DNI", "SS");
                eString = StringUtils.replace(eString, "-", "");
                rString = StringUtils.replace(rString, "-", "");
            } else if (editLine.endsWith("IND")) {
                editLine = StringUtils.replace(editLine, "IND", "SS");
                eString = StringUtils.replace(eString, "-", "");
                rString = StringUtils.replace(rString, "-", "");
            }
            if (StringUtils.countMatches(editLine, "S") == 2) {
                mc1 = editLine.indexOf("S");
                mt1 = "S";
                if (rc) {
                    mc1 = r.length() - 1 - mc1;
                }
                v1 = r.substring(mc1, mc1 + 1);
                editLine = StringUtils.replaceFirst(editLine, "S", StringUtils.repeat("X", 1));
                mc2 = StringUtils.indexOf(editLine, "S");
                mt2 = "S";
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
                v2 = r.substring(mc2, mc2 + 1);
            } else if (StringUtils.countMatches(editLine, "TT") == 2) {
                mc1 = editLine.indexOf("TT");
                mt1 = "TT";
                v1 = r.substring(mc1, mc1 + 2);
                editLine = StringUtils.replaceFirst(editLine, "TT", StringUtils.repeat("X", 2));
                mc2 = StringUtils.indexOf(editLine, "TT");
                mt2 = "TT";
                v2 = r.substring(mc2, mc2 + 2);
            } else if (StringUtils.countMatches(editLine, "I") == 2) {
                int iIndex = eString.indexOf("-");
                mc1 = iIndex;
                v1 = rString.substring(iIndex, iIndex + 1);
                mt1 = "I";
                eString = StringUtils.replaceFirst(eString, "-", StringUtils.repeat("X", 1));
                iIndex = eString.indexOf("-");
                mc2 = iIndex;
                v2 = rString.substring(iIndex, iIndex + 1);
                mt2 = "I";
            } else if (StringUtils.countMatches(editLine, "D") == 2) {
                int dIndex = rString.indexOf("-");
                mc1 = dIndex;
                v1 = eString.substring(dIndex, dIndex + 1);
                mt1 = "D";
                rString = StringUtils.replaceFirst(rString, "-", StringUtils.repeat("X", 1));
                dIndex = rString.indexOf("-");
                mc2 = dIndex;
                v2 = eString.substring(dIndex, dIndex + 1);
                mt2 = "D";
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "TT") == 1) {
                mc1 = editLine.indexOf("S");
                mt1 = "S";
                if (rc) {
                    mc1 = r.length() - 1 - mc1;
                }
                v1 = r.substring(mc1, mc1 + 1);
                mc2 = editLine.indexOf("TT");
                mt2 = "TT";
                v2 = r.substring(mc2, mc2 + 2);
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                mc1 = eString.indexOf("-");
                v1 = rString.substring(mc1, mc1 + 1);
                mt1 = "I";
                mc2 = editLine.indexOf("S");
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
                v2 = rString.substring(mc2, mc2 + 1);
                mt2 = "S";
            } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                mc1 = rString.indexOf("-");
                v1 = eString.substring(mc1, mc1 + 1);
                mt1 = "D";
                mc2 = editLine.indexOf("S");
                v2 = rString.substring(mc2, mc2 + 1);
                if (mc2 > mc1) {
                    mc2--;
                }
                mt2 = "S";
                if (rc) {
                    mc2 = r.length() - 1 - mc2;
                }
            } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                mc1 = eString.indexOf("-");
                v1 = rString.substring(mc1, mc1 + 1);
                mt1 = "I";
                mc2 = editLine.indexOf("TT");
                v2 = rString.substring(mc2, mc2 + 2);
                mt2 = "TT";
            } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                mc1 = rString.indexOf("-");
                v1 = eString.substring(mc1, mc1 + 1);
                mt1 = "D";
                mc2 = editLine.indexOf("TT");
                v2 = rString.substring(mc2, mc2 + 2);
                if (mc2 > mc1) {
                    mc2--;
                }
                mt2 = "TT";
            } else if (StringUtils.countMatches(editLine, "I") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                int iIndex = eString.indexOf("-");
                v1 = rString.substring(iIndex, iIndex + 1);
                mt1 = "I";
                int dIndex = rString.indexOf("-");
                v2 = eString.substring(dIndex, dIndex + 1);
                mt2 = "D";
                if (iIndex > dIndex) {
                    mc1 = iIndex - 1;
                    mc2 = dIndex;
                } else {
                    mc1 = iIndex;
                    mc2 = dIndex;
                }
            }

            if (!(v1.equals("") && v2.equals(""))) {
                Transcriptalignment2mismatch ta = new Transcriptalignment2mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + r.length(), readCount,
                        ledr.getDistance(), mc1, mt1, v1, mc2, mt2, v2);

                Transcriptalignment2mismatchtranspose tat = TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment2mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT2MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist1S(Sequence es, Long tid, Long eid, UUID rid, Sequence rs, Long readCount, Integer length, boolean rc, BatchStatement batch) {

            if (es != null && rs != null && identity != null) {
                Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 10, 10);
                float score = readAlignment.getScore();
                int start = readAlignment.getStart1();
                if (readAlignment.getStart2() > 0) {
                    start -= readAlignment.getStart2();
                }
                String ref = es.getSequence().substring(start, start + rs.getSequence().length());
                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                        = DamerauLevenshteinEditDistance.compute(ref, rs.getSequence());
                String editLine = ledr.getEditSequence();

                Integer m1c = 0;
                String mt = "";
                String v = "";
                if (editLine.contains("S")) {
                    m1c = editLine.indexOf("S");
                    mt = "S";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                } else if (editLine.contains("TT")) {
                    m1c = editLine.indexOf("TT");
                    mt = "TT";
                    v = rs.getSequence().substring(m1c, m1c + 2);
                }
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + rs.getSequence().length(), readCount,
                        ledr.getDistance(), m1c, mt, v);

                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }

        void persist1ID(Sequence es, Long tid, Long eid, UUID rid, Sequence rs, Long readCount, Integer length, boolean rc, BatchStatement batch) {

            if (es != null && rs != null && identity != null) {
                Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 0.5f, 10);
                float score = readAlignment.getScore();
                int start = readAlignment.getStart1();
                if (readAlignment.getStart2() > 0) {
                    start -= readAlignment.getStart2();
                }
                String ref = es.getSequence().substring(start, start + rs.getSequence().length());
                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                        = DamerauLevenshteinEditDistance.compute(ref, rs.getSequence());
                String editLine = ledr.getEditSequence();

                Integer m1c = 0;
                String mt = "";
                String v = "";
                int dIndex = 0;
                int iIndex = 0;
                if (editLine.contains("I")) {
                    iIndex = editLine.indexOf("I");
                }

                if (editLine.contains("D")) {
                    dIndex = editLine.indexOf("D");
                }

                if (dIndex < iIndex) {
                    m1c = dIndex;
                    mt = "D";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                } else if (iIndex < dIndex) {
                    m1c = iIndex;
                    mt = "I";
                    v = rs.getSequence().substring(m1c, m1c + 1);
                }
                Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                        rid, rc, start, start + rs.getSequence().length(), readCount,
                        ledr.getDistance(), m1c, mt, v);

                Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, rid, tid);
                if (tat == null) {
                    tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, rid, 1);
                } else {
                    int _c = tat.getCount();
                    _c++;
                    tat.setCount(_c);
                }

                batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
            }
        }
    }
}
