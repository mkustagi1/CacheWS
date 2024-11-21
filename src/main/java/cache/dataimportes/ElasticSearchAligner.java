package cache.dataimportes;

import cache.dataimportes.model.Reads;
import cache.dataimportes.model.Transcriptalignment;
import cache.dataimportes.model.Transcriptalignment1mismatch;
import cache.dataimportes.model.Transcriptalignment1mismatchtranspose;
import cache.dataimportes.model.Transcriptalignment2mismatch;
import cache.dataimportes.model.Transcriptalignment2mismatchtranspose;
import cache.dataimportes.model.Transcriptalignmenttranspose;
import cache.dataimportes.model.Unmappedreads;
import cache.dataimportes.util.ConsumerService;
import cache.dataimportes.util.DamerauLevenshteinEditDistance;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSMAPPER;
import static cache.dataimportes.util.DataAccess.READSMAPPER;
import static cache.dataimportes.util.DataAccess.READSQUERY;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTRANSPOSEMAPPER;
import static cache.dataimportes.util.DataAccess.UNMAPPEDREADSMAPPER;
import cache.dataimportes.util.Fuzzy;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import com.datastax.driver.core.utils.UUIDs;
import jaligner.Alignment;
import jaligner.Sequence;
import jaligner.SmithWatermanGotoh;
import jaligner.matrix.Matrix;
import jaligner.matrix.MatrixLoader;
import jaligner.matrix.MatrixLoaderException;
import jaligner.util.SequenceParser;
import jaligner.util.SequenceParserException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.StringUtils;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.views.ComplementSequenceView;
import org.biojava.nbio.core.sequence.views.ReversedSequenceView;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchAligner {

    protected static Matrix identity;
    static final Map<Long, String> TRANSCRIPTS = new HashMap<>();

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
        System.setProperty("org.apache.lucene.maxClauseCount", Integer.toString(Integer.MAX_VALUE));
        try {
            identity = MatrixLoader.load("IDENTITY");
        } catch (MatrixLoaderException mle) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, mle);
        }
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    enum TYPE {
        TRANSCRIPTS,
        READS
    }

    void init(List<Integer> indices, TYPE t) {
        if (TRANSCRIPTS.isEmpty()) {
            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from("cache", "transcript")
                    .where(eq("key", 1));
            ResultSet rs1 = SESSION.execute(stmt);
            while (!rs1.isExhausted()) {
                Row row = rs1.one();
                Long tid = row.getLong("transcriptid");
                String s = row.getString("sequence");
                TRANSCRIPTS.put(tid, s);
            }
        }
    }

    public void runAlignmentExactMatch(Long experimentId, Integer index) {
        try {
            Long rowCount = DATASTATISTICSMAPPER.get(experimentId).getTotalreadsnonredundant();
            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 100;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            Statement stmt = QueryBuilder
                    .select()
                    .column("id")
                    .from("cache", "reads")
                    .where(eq("experimentid", experimentId))
                    .orderBy(QueryBuilder.asc("id"))
                    .setFetchSize(60000);
            ResultSet rs = SESSION.execute(stmt);

            int i = 0, tCount = 0;
            Map<Integer, UUID[]> bounds = new HashMap<>();
            UUID min = null, max = null;
            while (!rs.isExhausted()) {
                Row row = rs.one();

                if (i == 0) {
                    min = row.getUUID("id");
                } else if (i == (tCount + 1) * batchSize) {
                    max = row.getUUID("id");
                    bounds.put(tCount, new UUID[]{min, max});
                    tCount++;
                    min = max;
                } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                    max = row.getUUID("id");
                    bounds.put((numThreads - 1), new UUID[]{min, max});
                }

                i++;
            }

            ExecutorService ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            List<Callable<Object>> callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                UUID[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImplReads t = new CallableImplReads(experimentId, j, range[0], range[1], index);
                callables.add(t);
            }

            System.out.println("Invoking callables");
            ex.invokeAll(callables);
            ex.shutdown();
        } catch (NumberFormatException | InterruptedException e) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public void runAlignmentSubstitutions(Long experimentId, Integer index) {
        try {

            Statement countStmt = QueryBuilder
                    .select()
                    .countAll()
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId));

            ResultSet rcs = SESSION.execute(countStmt.setReadTimeoutMillis(120000));
            Long rowCount = rcs.all().get(0).getLong(0);

            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 200;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            Statement stmt = QueryBuilder
                    .select()
                    .column("readid")
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId))
                    .orderBy(QueryBuilder.asc("readid"))
                    .setFetchSize(60000)
                    .setReadTimeoutMillis(Integer.MAX_VALUE);
            ResultSet rs = SESSION.execute(stmt);

            int i = 0, tCount = 0;
            Map<Integer, UUID[]> bounds = new HashMap<>();
            UUID min = null, max = null;
            while (!rs.isExhausted()) {
                Row row = rs.one();

                if (i == 0) {
                    min = row.getUUID("readid");
                } else if (i == (tCount + 1) * batchSize) {
                    max = row.getUUID("readid");
                    bounds.put(tCount, new UUID[]{min, max});
                    tCount++;
                    min = max;
                } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                    max = row.getUUID("readid");
                    bounds.put((numThreads - 1), new UUID[]{min, max});
                }

                i++;
            }

            ExecutorService ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            Settings settings = Settings.builder()
                    .put("cluster.name", "cache")
                    .put("client.transport.sniff", true).build();
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache01"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache02"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache03"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache04"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache05"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache06"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache07"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache08"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache09"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache10"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("cache11"), 9300));

            List<Callable<Object>> callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                UUID[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImplReadsSubstitutions t
                        = new CallableImplReadsSubstitutions(experimentId, j, range[0], range[1], index, client);
                callables.add(t);
            }

            System.out.println("Invoking callables");
            ex.invokeAll(callables);
            ex.shutdown();
            client.close();
            shutdown();
        } catch (UnknownHostException | NumberFormatException | InterruptedException e) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public void runSecondaryAlignment(long experimentId, Integer primaryIndex, Integer secondaryIndex) {
        try {
            Statement countStmt = QueryBuilder
                    .select()
                    .countAll()
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId));

            ResultSet rcs = SESSION.execute(countStmt.setReadTimeoutMillis(120000));
            Long rowCount = rcs.all().get(0).getLong(0);

            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 200;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            Statement stmt = QueryBuilder
                    .select()
                    .column("readid")
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId))
                    .orderBy(QueryBuilder.asc("readid"))
                    .setFetchSize(60000)
                    .setReadTimeoutMillis(Integer.MAX_VALUE);
            ResultSet rs = SESSION.execute(stmt);

            int i = 0, tCount = 0;
            Map<Integer, UUID[]> bounds = new HashMap<>();
            UUID min = null, max = null;
            while (!rs.isExhausted()) {
                Row row = rs.one();

                if (i == 0) {
                    min = row.getUUID("readid");
                } else if (i == (tCount + 1) * batchSize) {
                    max = row.getUUID("readid");
                    bounds.put(tCount, new UUID[]{min, max});
                    tCount++;
                    min = max;
                } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                    max = row.getUUID("readid");
                    bounds.put((numThreads - 1), new UUID[]{min, max});
                }

                i++;
            }

            ExecutorService ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            if (bounds.size() > 0) {
                Settings settings = Settings.builder()
                        .put("cluster.name", "cache")
                        .put("client.transport.sniff", true).build();
                TransportClient client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache01"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache02"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache03"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache04"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache05"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache06"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache07"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache08"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache09"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache10"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache11"), 9300));

                List<Callable<Object>> callables = new ArrayList<>();
                for (int j = 0; j < numThreads; j++) {
                    UUID[] range = bounds.get(j);
                    System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                    CallableImplSecondaryAlignments t
                            = new CallableImplSecondaryAlignments(experimentId, j, range[0], range[1], primaryIndex, client);
                    callables.add(t);
                }

                System.out.println("Invoking callables");
                ex.invokeAll(callables);
                ex.shutdown();
                client.close();
            }
        } catch (UnknownHostException | InterruptedException ex1) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex1);
        }
    }

    public void runSecondaryAlignmentTranscript(long experimentId, Integer primaryIndex, Integer secondaryIndex) {
        try {
            Statement countStmt = QueryBuilder
                    .select()
                    .countAll()
                    .from("cache", "transcript")
                    .where(eq("key", 1));

            ResultSet rcs = SESSION.execute(countStmt.setReadTimeoutMillis(120000));
            Long rowCount = rcs.all().get(0).getLong(0);

            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 100;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            List<UUID> unmappedReads = new ArrayList<>();

            Statement stmt = QueryBuilder
                    .select()
                    .column("readid")
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId))
                    .orderBy(QueryBuilder.asc("readid"))
                    .setFetchSize(60000)
                    .setReadTimeoutMillis(Integer.MAX_VALUE);
            ResultSet rs = SESSION.execute(stmt);

            while (!rs.isExhausted()) {
                Row row = rs.one();
                unmappedReads.add(row.getUUID("readid"));
            }

            Collections.sort(unmappedReads);

            stmt = QueryBuilder
                    .select()
                    .column("transcriptid")
                    .from("cache", "transcript")
                    .where(eq("key", 1))
                    .orderBy(QueryBuilder.asc("transcriptid"))
                    .setFetchSize(60000)
                    .setReadTimeoutMillis(Integer.MAX_VALUE);
            rs = SESSION.execute(stmt);

            int i = 0, tCount = 0;
            Map<Integer, Long[]> bounds = new HashMap<>();
            Long min = null, max = null;
            while (!rs.isExhausted()) {
                Row row = rs.one();
                if (i == 0) {
                    min = row.getLong("transcriptid");
                } else if (i == (tCount + 1) * batchSize) {
                    max = row.getLong("transcriptid");
                    bounds.put(tCount, new Long[]{min, max});
                    tCount++;
                    min = max;
                } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                    max = row.getLong("transcriptid");
                    bounds.put((numThreads - 1), new Long[]{min, max});
                }
                i++;
            }

            ExecutorService ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            if (bounds.size() > 0 && unmappedReads.size() > 0) {
                Settings settings = Settings.builder()
                        .put("cluster.name", "cache")
                        .put("client.transport.sniff", true).build();
                TransportClient client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache01"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache02"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache03"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache04"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache05"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache06"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache07"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache08"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache09"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache10"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache11"), 9300));

                List<Callable<Object>> callables = new ArrayList<>();
                for (int j = 0; j < numThreads; j++) {
                    Long[] range = bounds.get(j);
                    System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                    CallableImplTranscriptsTokens t
                            = new CallableImplTranscriptsTokens(experimentId, j, range[0], range[1], primaryIndex, secondaryIndex, client, unmappedReads);
                    callables.add(t);
                }

                System.out.println("Invoking callables");
                ex.invokeAll(callables);
                ex.shutdown();
                client.close();
            }
        } catch (Exception ex1) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex1);
        }
    }

    public static void shutdown() {
//        SESSION.close();
//        CLUSTER.close();
    }

    class CallableImplReads implements Callable {

        Long experimentId;
        UUID min;
        UUID max;
        Integer index;
        int threadId;
        TransportClient client;
        SearchRequestBuilder srb;

        public CallableImplReads(long experimentId, int threadId, UUID min, UUID max, Integer index) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.index = index;
                Settings settings = Settings.builder()
                        .put("cluster.name", "cache")
                        .put("client.transport.sniff", true).build();
                client = new PreBuiltTransportClient(settings)
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache01"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache02"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache03"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache04"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache05"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache06"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache07"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache08"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache09"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache10"), 9300))
                        .addTransportAddress(new TransportAddress(InetAddress.getByName("cache11"), 9300));

                srb = client.prepareSearch("index_transcripts_" + index)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setTypes("record")
                        .storedFields("transcriptid")
                        .setRouting("1")
                        .setExplain(false);
            } catch (Exception ex) {
                Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public Object call() {
            System.out.println("called threadId: " + threadId);
            final Object lock = new Object();

            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .column("id")
                    .column("sequence")
                    .from("cache", "reads")
                    .where(eq("experimentid", experimentId))
                    .and(gt("id", min))
                    .and(lte("id", max))
                    .setFetchSize(100000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("id")
                        .column("sequence")
                        .from("cache", "reads")
                        .where(eq("experimentid", experimentId))
                        .and(gte("id", min))
                        .and(lte("id", max))
                        .setFetchSize(100000);
            }

            System.out.println("line 519..");
            ResultSet rs = SESSION.execute(stmt);
            System.out.println("line 521..");

            int count = 0;
            BatchStatement batch = new BatchStatement();
            System.out.println("line 523..");
            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    UUID rid = row.getUUID("id");
                    String _s = row.getString("sequence");
                    Long readCount = row.getLong("count");
//                    Reads reads = READSMAPPER.get(experimentId, rid);
//                    String _s = reads.getSequence();
                    DNASequence sequence = new DNASequence(_s);
                    org.biojava.nbio.core.sequence.template.Sequence rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    SearchResponse response = srb.setQuery(QueryBuilders.matchQuery("sequence", _s)).get();
                    boolean mapped = false;
                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String s = TRANSCRIPTS.get(tid);
                        int sc = StringUtils.indexOf(s, _s);
                        int cm = StringUtils.countMatches(s, _s);
                        if (sc >= 0) {
                            Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                    sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                            Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                            if (tat == null) {
                                tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                        }
                        if (cm > 1) {
                            while (sc >= 0) {
                                s = StringUtils.replaceFirst(s, _s, StringUtils.repeat("X", _s.length()));
                                sc = StringUtils.indexOf(s, _s);
                                if (sc >= 0) {
                                    Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                            sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                                    Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                    if (tat == null) {
                                        tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                    }

                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", revComp)).get();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        String s = TRANSCRIPTS.get(tid);
                        int sc = StringUtils.indexOf(s, revComp);
                        int cm = StringUtils.countMatches(s, revComp);
                        if (sc >= 0) {
                            Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                    sc, sc + revComp.length(), readCount, ((Integer) revComp.length()).doubleValue());
                            Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                            if (tat == null) {
                                tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                        }
                        if (cm > 1) {
                            while (sc >= 0) {
                                s = StringUtils.replaceFirst(s, revComp, StringUtils.repeat("X", revComp.length()));
                                sc = StringUtils.indexOf(s, revComp);
                                if (sc >= 0) {
                                    Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                            sc, sc + revComp.length(), readCount, ((Integer) revComp.length()).doubleValue());
                                    Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                    if (tat == null) {
                                        tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                    }

                    if (!mapped) {
                        Unmappedreads umr = new Unmappedreads(experimentId, rid);
                        batch.add(UNMAPPEDREADSMAPPER.saveQuery(umr));
                    }
                    if (batch.size() > 1000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                    if (count % 10000 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("ElasticSearchAligner: Aligned " + count + " reads from experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (CompoundNotFoundException ex) {
                    Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All reads in experiment " + experimentId + " on thread " + threadId + " are aligned, it took " + seconds + " seconds");

            client.close();
            return threadId;
        }
    }

    class CallableImplReadsSubstitutions implements Callable {

        Long experimentId;
        UUID min;
        UUID max;
        Integer index;
        int threadId;
        TransportClient client;
        SearchRequestBuilder srb;

        public CallableImplReadsSubstitutions(long experimentId, int threadId, UUID min, UUID max, Integer index, TransportClient c) {
            this.experimentId = experimentId;
            this.threadId = threadId;
            this.min = min;
            this.max = max;
            this.index = index;
            this.client = c;

            srb = client.prepareSearch("index_transcripts_" + index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("transcriptid")
                    .setRouting("1")
                    .setExplain(false);
        }

        @Override
        public Object call() {
            final Object lock = new Object();

            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .column("readid")
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId))
                    .and(gt("readid", min))
                    .and(lte("readid", max))
                    .setFetchSize(100000);
            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("readid")
                        .from("cache", "unmappedreads")
                        .where(eq("experimentid", experimentId))
                        .and(gte("readid", min))
                        .and(lte("readid", max))
                        .setFetchSize(100000);
            }

            ResultSet rs = SESSION.execute(stmt);

            int count = 0;

            BatchStatement batch = new BatchStatement();
            long tookTime = 0;
            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    UUID rid = row.getUUID("readid");

                    Statement stmt1 = QueryBuilder
                            .select()
                            .column("sequence")
                            .from("cache", "reads")
                            .where(eq("experimentid", experimentId))
                            .and(eq("id", rid));
                    ResultSet rs1 = SESSION.execute(stmt1);
                    String _s = rs1.one().getString("sequence");

                    DNASequence sequence = new DNASequence(_s);
                    org.biojava.nbio.core.sequence.template.Sequence rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

//                    SearchResponse response = srb.setQuery(QueryBuilders.fuzzyQuery("sequence", _s).fuzziness(Fuzziness.ONE).prefixLength(2)).get();
                    SearchResponse response = srb.setQuery(QueryBuilders.matchQuery("sequence", _s).fuzziness(Fuzziness.ONE).prefixLength(2).operator(Operator.OR)).get();
                    tookTime += response.getTook().getMillis();

                    boolean mapped = false;
                    Fuzzy fuzzy = new Fuzzy();
                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());

                        String e = TRANSCRIPTS.get(tid);

                        double distance = fuzzy.containability(e, _s) * _s.length();
                        if (distance == 1.0) {
                            int st = fuzzy.getResultStart() - 1;
                            int end = st + _s.length();
                            persist1DL(e.substring(st, end), st, tid, experimentId, rid, _s, index, false, batch);
                            mapped = true;
                        }
                    }
//                    if (mapped) {
//                        batch.add(UNMAPPEDREADSMAPPER.deleteQuery(new Unmappedreads(experimentId, rid)));
//                    }

//                    response = srb.setQuery(QueryBuilders.fuzzyQuery("sequence", revComp).fuzziness(Fuzziness.ONE).prefixLength(2)).get();
                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", revComp).fuzziness(Fuzziness.ONE).prefixLength(2).operator(Operator.OR)).get();
                    tookTime += response.getTook().getMillis();

                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());

                        String e = TRANSCRIPTS.get(tid);

                        double distance = fuzzy.containability(e, revComp) * revComp.length();
                        if (distance == 1.0) {
                            int st = fuzzy.getResultStart() - 1;
                            int end = st + revComp.length();
                            persist1DL(e.substring(st, end), st, tid, experimentId, rid, revComp, index, true, batch);
                            mapped = true;
                        }
                    }
                    if (mapped) {
                        batch.add(UNMAPPEDREADSMAPPER.deleteQuery(new Unmappedreads(experimentId, rid)));
                    }

                    if (batch.size() > 3000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                } catch (CompoundNotFoundException | NumberFormatException ex) {
                    Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (count % 5000 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("Took Time: " + tookTime);
                    tookTime = 0;
                    System.out.println("ElasticSearchAligner: Aligned " + count + " reads from experiment "
                            + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                }
                count++;
            }

            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);

            System.out.println(
                    "All reads in experiment " + experimentId + " on thread " + threadId + " are aligned, it took " + seconds + " seconds");

            return threadId;
        }

        void persist1DL(String e, int start, Long tid, Long eid, UUID rid, String r, Integer length, boolean rc, BatchStatement batch) {
            DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                    = DamerauLevenshteinEditDistance.compute(e, r);
            String editLine = ledr.getEditSequence();

            Integer m1c = 0;
            String mt = "";
            String v = "";
            if (editLine.contains("S")) {
                m1c = editLine.indexOf("S");
                mt = "S";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("TT")) {
                m1c = editLine.indexOf("TT");
                mt = "TT";
                v = r.substring(m1c, m1c + 2);
            } else if (editLine.contains("I")) {
                m1c = editLine.indexOf("I");
                mt = "I";
                v = r.substring(m1c, m1c + 1);
            } else if (editLine.contains("D")) {
                m1c = editLine.indexOf("D");
                mt = "D";
                v = r.substring(m1c, m1c + 1);
            }

//            Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDGenerator.generate(),
//                    rid, rc, start, start + r.length(),
//                    ledr.getDistance(), m1c, mt, v);
//
//            Transcriptalignment1mismatchtranspose tat = new Transcriptalignment1mismatchtranspose(eid, tid, rid);
//            batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
//            batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
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

        Long experimentId;
        UUID min;
        UUID max;
        Integer index;
        int threadId;
        TransportClient client;
        SearchRequestBuilder srb;

        public CallableImplSecondaryAlignments(long experimentId, int threadId, UUID min, UUID max, Integer index, TransportClient c) {
            this.experimentId = experimentId;
            this.threadId = threadId;
            this.min = min;
            this.max = max;
            this.index = index;
            this.client = c;

            srb = client.prepareSearch("index_transcripts_" + index)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .storedFields("transcriptid")
                    .setRouting("1")
                    .setExplain(false);
        }

        @Override
        public Object call() {
            final Object lock = new Object();

            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .column("readid")
                    .from("cache", "unmappedreads")
                    .where(eq("experimentid", experimentId))
                    .and(gt("readid", min))
                    .and(lte("readid", max))
                    .setFetchSize(100000);
            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("readid")
                        .from("cache", "unmappedreads")
                        .where(eq("experimentid", experimentId))
                        .and(gte("readid", min))
                        .and(lte("readid", max))
                        .setFetchSize(100000);
            }

            ResultSet rs = SESSION.execute(stmt);

            int count = 0;

            BatchStatement batch = new BatchStatement();
            long tookTime = 0;
            while (!rs.isExhausted()) {
                String e = "";
                try {
                    Row row = rs.one();
                    UUID rid = row.getUUID("readid");

//                    Statement stmt1 = QueryBuilder
//                            .select()
//                            .column("sequence")
//                            .from("cache", "reads")
//                            .where(eq("experimentid", experimentId))
//                            .and(eq("id", rid));
//                    ResultSet rs1 = session.execute(stmt1);
//                    String _s = rs1.one().getString("sequence");
//                    Reads r = READSMAPPER.get(experimentId, rid);
                    BoundStatement bs = READSQUERY.bind().setLong("ei", experimentId).setUUID("ri", rid);
                    Row _r = SESSION.execute(bs).one();

                    String _s = _r.getString("sequence");
                    Long readCount = _r.getLong("count");

                    DNASequence sequence = new DNASequence(_s);
                    org.biojava.nbio.core.sequence.template.Sequence rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(sequence));
                    String revComp = rc.getSequenceAsString();

                    boolean mapped = false;
                    SearchResponse response = srb.setQuery(QueryBuilders.matchQuery("sequence", _s)).get();
                    tookTime += response.getTook().getMillis();
                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        e = TRANSCRIPTS.get(tid);
                        int sc = e.indexOf(_s);
                        int cm = StringUtils.countMatches(e, _s);
                        if (sc >= 0) {
                            Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                    sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                            Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                            if (tat == null) {
                                tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                while (sc >= 0) {
                                    e = StringUtils.replaceFirst(e, _s, StringUtils.repeat("X", _s.length()));
                                    sc = StringUtils.indexOf(e, _s);
                                    if (sc >= 0) {
                                        ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                                sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                                        tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                        if (tat == null) {
                                            tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                            e = TRANSCRIPTS.get(tid);
                            Fuzzy fuzzy = new Fuzzy();
                            double distance = fuzzy.containability(e, _s) * _s.length();
                            if (Math.rint(distance) == 1) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= e.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(e.substring(st, end), _s);
                                    if (ledr.getDistance() == 1.0) {
                                        persist1DL(ledr, e.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                        mapped = true;
                                    } else if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, e.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                        mapped = true;
                                    }
                                }
                            } else if (Math.rint(distance) == 2) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= e.length()) {
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(e.substring(st, end), _s);
                                    if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, e.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                        mapped = true;
                                    }
                                }
                            }
                        }
                    }

                    response = srb.setQuery(QueryBuilders.matchQuery("sequence", revComp)).get();
                    tookTime += response.getTook().getMillis();
                    for (SearchHit hit : response.getHits().getHits()) {
                        Long tid = Long.parseLong(hit.field("transcriptid").getValue().toString());
                        e = TRANSCRIPTS.get(tid);
                        int sc = e.indexOf(revComp);
                        int cm = StringUtils.countMatches(e, revComp);
                        if (sc >= 0) {
                            Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                    sc, sc + revComp.length(), readCount, ((Integer) revComp.length()).doubleValue());
                            Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                            if (tat == null) {
                                tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                while (sc >= 0) {
                                    e = StringUtils.replaceFirst(e, revComp, StringUtils.repeat("X", revComp.length()));
                                    sc = StringUtils.indexOf(e, revComp);
                                    if (sc >= 0) {
                                        ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                                sc, sc + revComp.length(), readCount, ((Integer) revComp.length()).doubleValue());
                                        tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                        if (tat == null) {
                                            tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                            e = TRANSCRIPTS.get(tid);
                            Fuzzy fuzzy = new Fuzzy();
                            double distance = fuzzy.containability(e, revComp) * revComp.length();
                            if (Math.rint(distance) == 1) {
                                int st = fuzzy.getResultStart() - 1;
                                int end = fuzzy.getResultEnd();
                                if (end <= e.length()) {
                                    e = e.substring(st, end);
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
                                if (end <= e.length()) {
                                    e = e.substring(st, end);
                                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                            = DamerauLevenshteinEditDistance.compute(e, revComp);
                                    if (ledr.getDistance() == 2.0) {
                                        persist2DL(ledr, e, st, tid, experimentId, rid, revComp, readCount, true, batch);
                                        mapped = true;
                                    }
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
                    Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (count % 5000 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("Took Time: " + tookTime);
                    tookTime = 0;
                    System.out.println("ElasticSearchAligner: Aligned " + count + " reads from experiment "
                            + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                }
                count++;
            }

            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);

            System.out.println(
                    "All reads in experiment " + experimentId + " on thread " + threadId + " are aligned, it took " + seconds + " seconds");

            return threadId;
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

    class CallableImplTranscripts implements Callable {

        Long experimentId;
        Long min;
        Long max;
        Integer primaryIndex;
        Integer secondaryIndex;
        int threadId;
        TransportClient client;
        Session session;
        SearchRequestBuilder srb;
        List<UUID> unmappedReads;

        public CallableImplTranscripts(long experimentId, int threadId, Long min, Long max,
                Integer primaryIndex, Integer secondaryIndex, Session s, TransportClient c, List<UUID> ur) {
            this.experimentId = experimentId;
            this.threadId = threadId;
            this.min = min;
            this.max = max;
            this.primaryIndex = primaryIndex;
            this.secondaryIndex = secondaryIndex;
            this.session = s;
            this.client = c;
            this.unmappedReads = ur;
            srb = client.prepareSearch("index_reads_" + secondaryIndex)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .setRouting(this.experimentId.toString())
                    .storedFields("id")
                    .setExplain(false);
        }

        @Override
        public Object call() {
            System.out.println("called threadid: " + threadId);
            final Object lock = new Object();
            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from("cache", "transcript")
                    .where(eq("key", 1))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(10);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .all()
                        .from("cache", "transcript")
                        .where(eq("key", 1))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(10);
            }
            ResultSet rs = session.execute(stmt);

            int i = 0;

            while (!rs.isExhausted()) {
                Row row = rs.one();
                Long tid = row.getLong("transcriptid");
                String seq = row.getString("sequence");
                if (seq.length() >= primaryIndex) {

                    try {
                        DNASequence sequence = new DNASequence(seq);
                        org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                                = new ReversedSequenceView<>(
                                        new ComplementSequenceView<>(sequence));
                        String revComp = rc.getSequenceAsString();

                        BatchStatement batch = new BatchStatement();

                        SearchResponse response = srb.setQuery(QueryBuilders
                                .boolQuery()
                                .must(QueryBuilders.matchQuery("eid", experimentId))
                                .must(QueryBuilders.matchQuery("sequence", seq))).get(); // Query

//                        BoolQueryBuilder q = QueryBuilders
//                                .boolQuery()
//                                .must(QueryBuilders.matchQuery("eid", experimentId))
//                                .must(QueryBuilders.boolQuery()
//                                        .should(QueryBuilders.boolQuery()
//                                                .must(QueryBuilders.matchQuery("eid", experimentId))
//                                                .must(QueryBuilders.matchQuery("sequence", seq).fuzziness(Fuzziness.TWO))
//                                                .must(QueryBuilders.matchQuery("length", primaryIndex))
//                                                .mustNot(QueryBuilders.boolQuery()
//                                                        .must(QueryBuilders.matchQuery("eid", experimentId))
//                                                        .must(QueryBuilders.matchQuery("sequence", seq))
//                                                        .must(QueryBuilders.matchQuery("length", primaryIndex))
//                                                ))
//                                        .should(QueryBuilders.boolQuery()
//                                                .must(QueryBuilders.matchQuery("eid", experimentId))
//                                                .must(QueryBuilders.matchQuery("sequence", seq))
//                                                .must(QueryBuilders.rangeQuery("length").gte(secondaryIndex).lt(primaryIndex))));
//                        SearchResponse response = srb.setQuery(q).get(); // Query
//                        if (response.getHits().totalHits() > 0) {
//                            System.out.println("Query:" + q.toString());
//                            System.out.println("response.size(), transcriptid: " + response.getHits().totalHits() + ", " + tid);
//                        }
                        for (SearchHit hit : response.getHits().getHits()) {
                            boolean mapped = false;
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            if (Collections.binarySearch(unmappedReads, rid) >= 0) {
                                Reads read = READSMAPPER.get(experimentId, rid);
                                String _s = read.getSequence();
                                Long readCount = read.getCount();
                                int sc = StringUtils.indexOf(seq, _s);
                                int cm = StringUtils.countMatches(seq, _s);
                                if (sc >= 0) {
                                    Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                            sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());

                                    Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                    if (tat == null) {
                                        tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                        while (sc >= 0) {
                                            seq = StringUtils.replaceFirst(seq, _s, StringUtils.repeat("X", _s.length()));
                                            sc = StringUtils.indexOf(seq, _s);
                                            if (sc >= 0) {
                                                ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                                        sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                                                tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                                if (tat == null) {
                                                    tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                    double distance = fuzzy.containability(seq, _s) * _s.length();
                                    if (Math.rint(distance) == 1) {
                                        int st = fuzzy.getResultStart() - 1;
                                        int end = fuzzy.getResultEnd();
                                        if (end <= seq.length()) {
                                            DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                    = DamerauLevenshteinEditDistance.compute(seq.substring(st, end), _s);
                                            if (ledr.getDistance() == 1.0) {
                                                persist1DL(ledr, seq.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                                mapped = true;
                                            } else if (ledr.getDistance() == 2.0) {
                                                persist2DL(ledr, seq.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                                mapped = true;
                                            }
                                        }
                                    } else if (Math.rint(distance) == 2) {
                                        int st = fuzzy.getResultStart() - 1;
                                        int end = fuzzy.getResultEnd();
                                        if (end <= seq.length()) {
                                            DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                    = DamerauLevenshteinEditDistance.compute(seq.substring(st, end), _s);
                                            if (ledr.getDistance() == 2.0) {
                                                persist2DL(ledr, seq.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                                mapped = true;
                                            }
                                        }
                                    }
                                }
                                if (mapped) {
                                    Unmappedreads umr = new Unmappedreads(experimentId, rid);
                                    batch.add(UNMAPPEDREADSMAPPER.deleteQuery(umr));
                                }
                                if (batch.size() >= 3000) {
//                                    session.execute(batch);
                                    batch.clear();
                                }
                            }
                        }

                        response = srb.setQuery(QueryBuilders
                                .boolQuery()
                                .must(QueryBuilders.matchQuery("eid", experimentId))
                                .must(QueryBuilders.matchQuery("sequence", revComp))).get(); // Query
//                        q = QueryBuilders
//                                .boolQuery()
//                                .must(QueryBuilders.matchQuery("eid", experimentId))
//                                .must(QueryBuilders.boolQuery()
//                                        .should(QueryBuilders.boolQuery()
//                                                .must(QueryBuilders.matchQuery("eid", experimentId))
//                                                .must(QueryBuilders.matchQuery("sequence", seq).fuzziness(Fuzziness.TWO))
//                                                .must(QueryBuilders.matchQuery("length", primaryIndex))
//                                                .mustNot(QueryBuilders.boolQuery()
//                                                        .must(QueryBuilders.matchQuery("eid", experimentId))
//                                                        .must(QueryBuilders.matchQuery("sequence", seq))
//                                                        .must(QueryBuilders.matchQuery("length", primaryIndex))
//                                                ))
//                                        .should(QueryBuilders.boolQuery()
//                                                .must(QueryBuilders.matchQuery("eid", experimentId))
//                                                .must(QueryBuilders.matchQuery("sequence", seq))
//                                                .must(QueryBuilders.rangeQuery("length").gte(secondaryIndex).lt(primaryIndex))));
//                        response = srb.setQuery(q).get(); // Query

//                        System.out.println("rc: response.size(), transcriptid: " + response.getHits().totalHits() + ", " + tid);
                        for (SearchHit hit : response.getHits().getHits()) {
                            boolean mapped = false;
                            UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                            if (Collections.binarySearch(unmappedReads, rid) >= 0) {
                                Reads read = READSMAPPER.get(experimentId, rid);
                                String _s = read.getSequence();
                                Long readCount = read.getCount();
                                int sc = StringUtils.indexOf(revComp, _s);
                                int cm = StringUtils.countMatches(revComp, _s);
                                if (sc >= 0) {
                                    Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                            sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                                    Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                    if (tat == null) {
                                        tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                        while (sc >= 0) {
                                            revComp = StringUtils.replaceFirst(revComp, _s, StringUtils.repeat("X", _s.length()));
                                            sc = StringUtils.indexOf(revComp, _s);
                                            if (sc >= 0) {
                                                ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                                        sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                                                tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                                if (tat == null) {
                                                    tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                    double distance = fuzzy.containability(seq, revComp) * revComp.length();
                                    if (Math.rint(distance) == 1) {
                                        int st = fuzzy.getResultStart() - 1;
                                        int end = fuzzy.getResultEnd();
                                        if (end <= seq.length()) {
                                            seq = seq.substring(st, end);
                                            DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                    = DamerauLevenshteinEditDistance.compute(seq, revComp);
                                            if (ledr.getDistance() == 1.0) {
                                                persist1DL(ledr, seq, st, tid, experimentId, rid, revComp, readCount, true, batch);
                                                mapped = true;
                                            } else if (ledr.getDistance() == 2.0) {
                                                persist2DL(ledr, seq, st, tid, experimentId, rid, revComp, readCount, true, batch);
                                                mapped = true;
                                            }
                                        }
                                    } else if (Math.rint(distance) == 2) {
                                        int st = fuzzy.getResultStart() - 1;
                                        int end = fuzzy.getResultEnd();
                                        if (end <= seq.length()) {
                                            seq = seq.substring(st, end);
                                            DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                    = DamerauLevenshteinEditDistance.compute(seq, revComp);
                                            if (ledr.getDistance() == 2.0) {
                                                persist2DL(ledr, seq, st, tid, experimentId, rid, revComp, readCount, true, batch);
                                                mapped = true;
                                            }
                                        }
                                    }
                                }
                                if (mapped) {
                                    Unmappedreads umr = new Unmappedreads(experimentId, rid);
                                    batch.add(UNMAPPEDREADSMAPPER.deleteQuery(umr));
                                }
                                if (batch.size() >= 3000) {
//                                    session.execute(batch);
                                    batch.clear();
                                }
                            }
                        }
                        if (batch.size() > 0) {
//                            session.execute(batch);
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                if (i % 10 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("ElasticSearchAligner: Aligned " + i + " transcripts to experiment "
                            + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                }
                i++;
            }
            return threadId;
        }

        void persist1S(final String transcript, Long tid, Long eid, List<String> ids, Integer length, boolean rc) {

            final Object lock = new Object();
            BatchStatement batch = new BatchStatement();

            Spliterator<String> k5 = ids.spliterator();
            Consumer<String> actionIds = (String id) -> {
                try {
                    String _transcript = transcript;
                    Reads r = READSMAPPER.get(eid, UUID.fromString(id));
                    String s = r.getSequence();
                    Sequence rs = SequenceParser.parse(s);
                    Sequence es = SequenceParser.parse(_transcript);

                    if (es != null && rs != null && identity != null) {
                        Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 10, 10);
                        float score = readAlignment.getScore();
                        int start = readAlignment.getStart1();
                        if (readAlignment.getStart2() > 0) {
                            start -= readAlignment.getStart2();
                        }
                        String ref = _transcript.substring(start, start + s.length());
                        String editLine = DamerauLevenshteinEditDistance.compute(ref, s).getEditSequence();

                        Integer m1c = 0;
                        String mt = "";
                        String v = "";
                        if (editLine.contains("S")) {
                            m1c = editLine.indexOf("S");
                            mt = "S";
                            v = s.substring(m1c, m1c + 1);
                        } else if (editLine.contains("TT")) {
                            m1c = editLine.indexOf("TT");
                            mt = "TT";
                            v = s.substring(m1c, m1c + 2);
                        }
                        Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                                r.getId(), rc, start, start + s.length(), r.getCount(),
                                score, m1c, mt, v);
                        if (rc) {
                            ta.setMismatchcoordinate(transcript.length() - ta.getMismatchcoordinate() - 1);
                        }

                        Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, r.getId(), tid);
                        if (tat == null) {
                            tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, r.getId(), 1);
                        } else {
                            int _c = tat.getCount();
                            _c++;
                            tat.setCount(_c);
                        }
                        synchronized (lock) {
                            batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                            batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
                        }
                    }
                } catch (SequenceParserException ex) {
                    Logger.getLogger(ElasticSearchAligner.class
                            .getName()).log(Level.SEVERE, null, ex);
                }
            };

            long targetBatchSize = k5.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
            new ParEach(null, k5, actionIds, targetBatchSize).invoke();

            SESSION.execute(batch);
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
    }

    class CallableImplTranscriptsTokens implements Callable {

        Long experimentId;
        Long min;
        Long max;
        Integer primaryIndex;
        Integer secondaryIndex;
        int threadId;
        TransportClient client;
        SearchRequestBuilder srb;
        List<UUID> unmappedReads;
        AnalyzeRequest request;

        public CallableImplTranscriptsTokens(long experimentId, int threadId, Long min, Long max,
                Integer primaryIndex, Integer secondaryIndex, TransportClient c, List<UUID> ur) {
            this.experimentId = experimentId;
            this.threadId = threadId;
            this.min = min;
            this.max = max;
            this.primaryIndex = primaryIndex;
            this.secondaryIndex = secondaryIndex;
            this.client = c;
            this.unmappedReads = ur;
            srb = client.prepareSearch("index_reads_" + secondaryIndex)
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setTypes("record")
                    .setRouting(this.experimentId.toString())
                    .storedFields("id")
                    .setExplain(false);
            String analyzer = "analyzer" + secondaryIndex.toString();
            request = (new AnalyzeRequest("index_reads_" + secondaryIndex.toString()))
                    .analyzer(analyzer);
        }

        @Override
        public Object call() {
            System.out.println("called threadid: " + threadId);
            final Object lock = new Object();
            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from("cache", "transcript")
                    .where(eq("key", 1))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(10);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .all()
                        .from("cache", "transcript")
                        .where(eq("key", 1))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(10);
            }
            ResultSet rs = SESSION.execute(stmt);

            int i = 0;

            while (!rs.isExhausted()) {
                Row row = rs.one();
                Long tid = row.getLong("transcriptid");
                String seq = row.getString("sequence");
                if (seq.length() >= primaryIndex) {

                    try {
                        DNASequence sequence = new DNASequence(seq);
                        org.biojava.nbio.core.sequence.template.Sequence<NucleotideCompound> rc
                                = new ReversedSequenceView<>(
                                        new ComplementSequenceView<>(sequence));
                        String revComp = rc.getSequenceAsString();

                        BatchStatement batch = new BatchStatement();

                        request.text(seq);
                        List<AnalyzeResponse.AnalyzeToken> tokens = client.admin()
                                .indices().analyze(request).actionGet().getTokens();
//                        System.out.println("Tokens.length(): " + tokens.size());
//                        int tc = 0;
                        for (AnalyzeResponse.AnalyzeToken token : tokens) {
                            int startOffset = token.getStartOffset();
                            if ((startOffset % secondaryIndex == 0) || ((startOffset + Math.rint(secondaryIndex / 2)) % secondaryIndex == 0)) {
                                int endOffset = token.getEndOffset();
                                String _seq = token.getTerm();
                                List<UUID> ids = new ArrayList<>();
                                SearchResponse response = srb.setQuery(QueryBuilders
                                        .boolQuery()
                                        .must(QueryBuilders.matchQuery("eid", experimentId))
                                        .must(QueryBuilders.termQuery("sequence", _seq))).get(); // Query
//                                System.out.println("Startoffset: "  + startOffset + " Token: " + tc++ + " of " + tokens.size() + " tid: "
//                                        + tid + " hits: " + response.getHits().hits().length);
                                for (SearchHit hit : response.getHits().getHits()) {
                                    boolean mapped = false;
                                    UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                                    if (!ids.contains(rid)) {
                                        ids.add(rid);
                                        if (Collections.binarySearch(unmappedReads, rid) >= 0) {
                                            Reads read = READSMAPPER.get(experimentId, rid);
                                            String _s = read.getSequence();
                                            Long readCount = read.getCount();
                                            int sc = StringUtils.indexOf(seq, _s);
                                            int cm = StringUtils.countMatches(seq, _s);
                                            if (sc >= 0) {
                                                Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                                        sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());

                                                Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                                if (tat == null) {
                                                    tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                                    while (sc >= 0) {
                                                        seq = StringUtils.replaceFirst(seq, _s, StringUtils.repeat("X", _s.length()));
                                                        sc = StringUtils.indexOf(seq, _s);
                                                        if (sc >= 0) {
                                                            ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, false,
                                                                    sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                                                            tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                                            if (tat == null) {
                                                                tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                                double distance = fuzzy.containability(seq, _s) * _s.length();
                                                if (Math.rint(distance) == 1) {
                                                    int st = fuzzy.getResultStart() - 1;
                                                    int end = fuzzy.getResultEnd();
                                                    if (end <= seq.length()) {
                                                        DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                                = DamerauLevenshteinEditDistance.compute(seq.substring(st, end), _s);
                                                        if (ledr.getDistance() == 1.0) {
                                                            persist1DL(ledr, seq.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                                            mapped = true;
                                                        } else if (ledr.getDistance() == 2.0) {
                                                            persist2DL(ledr, seq.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                                            mapped = true;
                                                        }
                                                    }
                                                } else if (Math.rint(distance) == 2) {
                                                    int st = fuzzy.getResultStart() - 1;
                                                    int end = fuzzy.getResultEnd();
                                                    if (end <= seq.length()) {
                                                        DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                                = DamerauLevenshteinEditDistance.compute(seq.substring(st, end), _s);
                                                        if (ledr.getDistance() == 2.0) {
                                                            persist2DL(ledr, seq.substring(st, end), st, tid, experimentId, rid, _s, readCount, false, batch);
                                                            mapped = true;
                                                        }
                                                    }
                                                }
                                            }
                                            if (mapped) {
                                                Unmappedreads umr = new Unmappedreads(experimentId, rid);
                                                batch.add(UNMAPPEDREADSMAPPER.deleteQuery(umr));
                                            }
                                            if (batch.size() >= 3000) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        request.text(revComp);
                        tokens = client.admin()
                                .indices().analyze(request).actionGet().getTokens();
                        for (AnalyzeResponse.AnalyzeToken token : tokens) {
                            int startOffset = token.getStartOffset();
                            if ((startOffset % secondaryIndex == 0) || ((startOffset + Math.rint(secondaryIndex / 2)) % secondaryIndex == 0)) {
                                int endOffset = token.getEndOffset();
                                String _revComp = token.getTerm();
                                List<UUID> ids = new ArrayList<>();
                                SearchResponse response = srb.setQuery(QueryBuilders
                                        .boolQuery()
                                        .must(QueryBuilders.matchQuery("eid", experimentId))
                                        .must(QueryBuilders.termQuery("sequence", _revComp))).get(); // Query
                                for (SearchHit hit : response.getHits().getHits()) {
                                    boolean mapped = false;
                                    UUID rid = UUID.fromString(hit.field("id").getValue().toString());
                                    if (!ids.contains(rid)) {
                                        ids.add(rid);
                                        if (Collections.binarySearch(unmappedReads, rid) >= 0) {
                                            Reads read = READSMAPPER.get(experimentId, rid);
                                            String _s = read.getSequence();
                                            Long readCount = read.getCount();
                                            int sc = StringUtils.indexOf(revComp, _s);
                                            int cm = StringUtils.countMatches(revComp, _s);
                                            if (sc >= 0) {
                                                Transcriptalignment ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                                        sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());

                                                Transcriptalignmenttranspose tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                                if (tat == null) {
                                                    tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                                    while (sc >= 0) {
                                                        revComp = StringUtils.replaceFirst(revComp, _s, StringUtils.repeat("X", _s.length()));
                                                        sc = StringUtils.indexOf(revComp, _s);
                                                        if (sc >= 0) {
                                                            ta = new Transcriptalignment(experimentId, tid, UUIDs.timeBased(), rid, true,
                                                                    sc, sc + _s.length(), readCount, ((Integer) _s.length()).doubleValue());
                                                            tat = TRANSCRIPTALIGNMENTTRANSPOSEMAPPER.get(experimentId, rid, tid);
                                                            if (tat == null) {
                                                                tat = new Transcriptalignmenttranspose(experimentId, tid, rid, 1);
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
                                                double distance = fuzzy.containability(revComp, _s) * _s.length();
                                                if (Math.rint(distance) == 1) {
                                                    int st = fuzzy.getResultStart() - 1;
                                                    int end = fuzzy.getResultEnd();
                                                    if (end <= revComp.length()) {
                                                        DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                                = DamerauLevenshteinEditDistance.compute(revComp.substring(st, end), _s);
                                                        if (ledr.getDistance() == 1.0) {
                                                            persist1DL(ledr, revComp.substring(st, end), st, tid, experimentId, rid, _s, readCount, true, batch);
                                                            mapped = true;
                                                        } else if (ledr.getDistance() == 2.0) {
                                                            persist2DL(ledr, revComp.substring(st, end), st, tid, experimentId, rid, _s, readCount, true, batch);
                                                            mapped = true;
                                                        }
                                                    }
                                                } else if (Math.rint(distance) == 2) {
                                                    int st = fuzzy.getResultStart() - 1;
                                                    int end = fuzzy.getResultEnd();
                                                    if (end <= seq.length()) {
                                                        DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                                                                = DamerauLevenshteinEditDistance.compute(revComp.substring(st, end), _s);
                                                        if (ledr.getDistance() == 2.0) {
                                                            persist2DL(ledr, revComp.substring(st, end), st, tid, experimentId, rid, _s, readCount, true, batch);
                                                            mapped = true;
                                                        }
                                                    }
                                                }
                                            }
                                            if (mapped) {
                                                Unmappedreads umr = new Unmappedreads(experimentId, rid);
                                                batch.add(UNMAPPEDREADSMAPPER.deleteQuery(umr));
                                            }
                                            if (batch.size() >= 3000) {
                                                SESSION.execute(batch);
                                                batch.clear();
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if (batch.size() > 0) {
//                            session.execute(batch);
                        }
                    } catch (Exception ex) {
                        Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                if (i % 10 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("ElasticSearchAligner: Aligned " + i + " transcripts to experiment "
                            + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                }
                i++;
            }
            return threadId;
        }

        void persist1S(final String transcript, Long tid, Long eid, List<String> ids, Integer length, boolean rc) {

            final Object lock = new Object();
            BatchStatement batch = new BatchStatement();

            Spliterator<String> k5 = ids.spliterator();
            Consumer<String> actionIds = (String id) -> {
                try {
                    String _transcript = transcript;
                    Reads r = READSMAPPER.get(eid, UUID.fromString(id));
                    String s = r.getSequence();
                    Long readCount = r.getCount();
                    Sequence rs = SequenceParser.parse(s);
                    Sequence es = SequenceParser.parse(_transcript);

                    if (es != null && rs != null && identity != null) {
                        Alignment readAlignment = SmithWatermanGotoh.align(es, rs, identity, 10, 10);
                        float score = readAlignment.getScore();
                        int start = readAlignment.getStart1();
                        if (readAlignment.getStart2() > 0) {
                            start -= readAlignment.getStart2();
                        }
                        String ref = _transcript.substring(start, start + s.length());
                        String editLine = DamerauLevenshteinEditDistance.compute(ref, s).getEditSequence();

                        Integer m1c = 0;
                        String mt = "";
                        String v = "";
                        if (editLine.contains("S")) {
                            m1c = editLine.indexOf("S");
                            mt = "S";
                            v = s.substring(m1c, m1c + 1);
                        } else if (editLine.contains("TT")) {
                            m1c = editLine.indexOf("TT");
                            mt = "TT";
                            v = s.substring(m1c, m1c + 2);
                        }
                        Transcriptalignment1mismatch ta = new Transcriptalignment1mismatch(eid, tid, UUIDs.timeBased(),
                                r.getId(), rc, start, start + s.length(), readCount,
                                score, m1c, mt, v);
                        if (rc) {
                            ta.setMismatchcoordinate(transcript.length() - ta.getMismatchcoordinate() - 1);
                        }

                        Transcriptalignment1mismatchtranspose tat = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.get(experimentId, r.getId(), tid);
                        if (tat == null) {
                            tat = new Transcriptalignment1mismatchtranspose(experimentId, tid, r.getId(), 1);
                        } else {
                            int _c = tat.getCount();
                            _c++;
                            tat.setCount(_c);
                        }

                        synchronized (lock) {
                            batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta));
                            batch.add(TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER.saveQuery(tat));
                        }
                    }
                } catch (SequenceParserException ex) {
                    Logger.getLogger(ElasticSearchAligner.class
                            .getName()).log(Level.SEVERE, null, ex);
                }
            };

            long targetBatchSize = k5.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
            new ParEach(null, k5, actionIds, targetBatchSize).invoke();

            SESSION.execute(batch);
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
    }

    class ParEach<T> extends CountedCompleter<Void> {

        final Spliterator<T> spliterator;
        final Consumer<T> action;
        final long targetBatchSize;

        ParEach(ParEach<T> parent, Spliterator<T> spliterator,
                Consumer<T> action, long targetBatchSize) {
            super(parent);
            this.spliterator = spliterator;
            this.action = action;
            this.targetBatchSize = targetBatchSize;
        }

        @Override
        public void compute() {
            Spliterator<T> sub;
            while (spliterator.estimateSize() > targetBatchSize
                    && (sub = spliterator.trySplit()) != null) {
                addToPendingCount(1);
                new ParEach<>(this, sub, action, targetBatchSize).fork();
            }
            spliterator.forEachRemaining(action);
            propagateCompletion();
        }
    }

    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            Map<Long, Integer> indexMap = new HashMap<>();
            String line = "";
            System.out.println(args[0]);
            System.out.println(ElasticSearchAligner.class.getName());
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0] + ", " + tokens[1]);
                indexMap.put(Long.parseLong(tokens[0]), Integer.parseInt(tokens[1]));
            }
            br.close();
            for (final Long key : indexMap.keySet()) {
                final String index;
                switch (indexMap.get(key)) {
                    case 101:
                        index = "101";
                        break;
                    case 100:
                        index = "100";
                        break;
                    case 50:
                        index = "50";
                        break;
                    case 75:
                        index = "75";
                        break;
                    case 65:
                        index = "65";
                        break;
                    case 20:
                        index = "20";
                        break;
                    case 97:
                        index = "97";
                        break;
                    default:
                        index = "101";
                        break;
                }

                Callable callable = (Callable) () -> {
                    ElasticSearchAligner sstp0 = new ElasticSearchAligner();
                    List<Integer> indices = new ArrayList<>();
//                    indices.add(101);
                    indices.add(40);
                    sstp0.init(indices, TYPE.TRANSCRIPTS);
//            sstp0.runAlignmentExactMatch(1l, 101);
                    sstp0.runSecondaryAlignment(key, 40, 40);
//                    sstp0.runSecondaryAlignmentTranscript(key, Integer.parseInt(index), 40);
//            sstp0.shutdown();
//            sstp0.init(indices, TYPE.READS);
//            sstp0.runSecondaryAlignment(1l, 101, 101);
                    return "All alignments in datasets " + key + " completed.";
                };
                CONSUMER_QUEUE.put(callable);
            }

//            Callable callable = (Callable) () -> {
//                ElasticSearchAligner sstp0 = new ElasticSearchAligner();
//                List<Integer> indices = new ArrayList<>();
//                indices.add(101);
////            indices.add(40);
//                sstp0.init(indices, TYPE.TRANSCRIPTS);
////            sstp0.runAlignmentExactMatch(1l, 101);
////                sstp0.runAlignmentSubstitutions(40l, 101);
//                sstp0.runSecondaryAlignment(1l, 40, 40);
////                sstp0.shutdown();
////            sstp0.init(indices, TYPE.READS);
////            sstp0.runSecondaryAlignment(1l, 101, 101);
//                return "All alignments in datasets " + 40l + " completed.";
//            };
//            CONSUMER_QUEUE.put(callable);
            Callable callable = (Callable) () -> {
                shutdown();
                return 1d;
            };
            CONSUMER_QUEUE.put(callable);
        } catch (InterruptedException ex) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ElasticSearchAligner.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
