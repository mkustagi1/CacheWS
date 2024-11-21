package cache.dataimportes;

import cache.dataimportes.holders.TranscriptMappingResults;
import cache.dataimportes.model.Transcript;
import cache.dataimportes.model.Transcriptalignment;
import cache.dataimportes.model.Transcriptalignment1mismatch;
import cache.dataimportes.model.Transcriptalignment2mismatch;
import cache.dataimportes.model.Transcriptalignmentbartelpolya;
import cache.dataimportes.model.Transcriptalignmentpolya;
import cache.dataimportes.model.Transcriptalignmentsummary;
import cache.dataimportes.model.Transcriptalignmenttss;
import cache.dataimportes.util.ConsumerService;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTBARTELPOLYAQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTPOLYAMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTPOLYAQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTSUMMARYMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTSSMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTSSQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTMAPPER;
import cache.dataimportes.util.Stack1Mismatch;
import cache.dataimportes.util.Stack2Mismatch;
import cache.dataimportes.util.StackExactMatch;
import cache.dataimportes.util.ZipUtil;
import static cache.dataimportes.util.ZipUtil.encode;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import static com.datastax.driver.core.ConsistencyLevel.ANY;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.mapping.Mapper.Option.consistencyLevel;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CreateTranscriptAlignmentSummaries {

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static final Map<Long, List<Transcriptalignmentpolya>> POLYAALIGNMENTS = new HashMap<>();
    static final Map<Long, List<Transcriptalignmentbartelpolya>> BARTELPOLYAALIGNMENTS = new HashMap<>();
    static final Map<Long, List<Transcriptalignmenttss>> TSSALIGNMENTS = new HashMap<>();
    static final Map<Long, Transcript> TRANSCRIPTS = new HashMap<>();

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, ex);
        }
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    public static void init() {
        Statement stmt = QueryBuilder
                .select()
                .column("transcriptid")
                .column("name")
                .from("cache", "transcript")
                .where(eq("key", 1))
                .setFetchSize(2000);
        ResultSet rs = SESSION.execute(stmt);
        int count = 0;
        long start = System.nanoTime();
        while (!rs.isExhausted()) {
            try {
                Row row = rs.one();
                Long tid = row.getLong("transcriptid");
                String name = row.getString("name");

                Transcript transcript = TRANSCRIPTMAPPER.get(1, tid, name);
                TRANSCRIPTS.put(tid, transcript);

                BoundStatement bs = TRANSCRIPTALIGNMENTPOLYAQUERY.bind().setLong("ti", tid);
                ResultSet _rs = SESSION.execute(bs);
                List<Transcriptalignmentpolya> talp = TRANSCRIPTALIGNMENTPOLYAMAPPER.map(_rs).all();
                POLYAALIGNMENTS.put(tid, talp);

                bs = TRANSCRIPTALIGNMENTBARTELPOLYAQUERY.bind().setLong("ti", tid);
                _rs = SESSION.execute(bs);
                List<Transcriptalignmentbartelpolya> talbp = TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER.map(_rs).all();
                BARTELPOLYAALIGNMENTS.put(tid, talbp);

                bs = TRANSCRIPTALIGNMENTTSSQUERY.bind().setLong("ti", tid);
                _rs = SESSION.execute(bs);
                List<Transcriptalignmenttss> taltss = TRANSCRIPTALIGNMENTTSSMAPPER.map(_rs).all();
                TSSALIGNMENTS.put(tid, taltss);
                if (count % 1000 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("CreateTranscriptAlignmentSummaries: Loaded meta alignments for " + count + " transcripts in " + seconds1 + " seconds");
                }
                count++;
            } catch (Exception e) {
                Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, e);
            }
        }
    }

    public void runThreads(Long experimentId, int distance) {
        try {
            Statement stmt = QueryBuilder
                    .select()
                    .countAll()
                    .from("cache", "transcript")
                    .where(eq("key", 1));

            ResultSet rs = SESSION.execute(stmt);
            Long rowCount = rs.one().getLong(0);
            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 60;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            stmt = QueryBuilder
                    .select()
                    .column("transcriptid")
                    .from("cache", "transcript")
                    .where(eq("key", 1))
                    .setFetchSize(2000);
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

            List<Callable<Object>> callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                Long[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                switch (distance) {
                    case 0:
                        CallableImplExactMatch t = new CallableImplExactMatch(experimentId, j, range[0], range[1]);
                        callables.add(t);
                        break;
                    case 1:
                        CallableImpl1Mismatch t1 = new CallableImpl1Mismatch(experimentId, j, range[0], range[1]);
                        callables.add(t1);
                        break;
                    case 2:
                        CallableImpl2Mismatch t2 = new CallableImpl2Mismatch(experimentId, j, range[0], range[1]);
                        callables.add(t2);
                        break;
                    default:
                        break;
                }

            }

            System.out.println("Invoking callables");
            ex.invokeAll(callables);
            ex.shutdown();
        } catch (NumberFormatException | InterruptedException e) {
            Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    class CallableImplExactMatch implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;

        public CallableImplExactMatch(long experimentId, int threadId, Long min, Long max) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
            } catch (Exception ex) {
                Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public Object call() {
            System.out.println("called threadId: " + threadId);
            final Object lock = new Object();

            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .column("transcriptid")
                    .from("cache", "transcript")
                    .where(eq("key", 1))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(1000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcript")
                        .where(eq("key", 1))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(1000);
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
                    Long tid = row.getLong("transcriptid");

                    Transcript transcript = TRANSCRIPTS.get(tid);

                    BoundStatement bs = TRANSCRIPTALIGNMENTQUERY.bind().setLong("ei", experimentId).setLong("ti", tid);
                    ResultSet _rs = SESSION.execute(bs);
                    List<Transcriptalignment> tal = TRANSCRIPTALIGNMENTMAPPER.map(_rs).all();

                    TranscriptMappingResults tmr = new TranscriptMappingResults();

                    StackExactMatch stack = new StackExactMatch();

                    tmr = stack.formatWithCAGEPolyAAndBartelPolyA(tmr, transcript, tal,
                            POLYAALIGNMENTS.get(tid), TSSALIGNMENTS.get(tid), BARTELPOLYAALIGNMENTS.get(tid));

                    Transcriptalignmentsummary tas = new Transcriptalignmentsummary();

                    tas.setExperimentid(experimentId);
                    tas.setTranscriptid(tid);
                    tas.setDistance(0);

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

                    batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.saveQuery(tas, consistencyLevel(ANY)));

                    if (batch.size() > 25) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                    if ((count) % 25 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("CreateTranscriptAlignmentSummaries: Created summaries for " + count + " transcripts in experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception e) {
                    Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All transcript summaries in experiment " + experimentId + " on thread " + threadId + " are created, it took " + seconds + " seconds");

            return threadId;
        }
    }

    class CallableImpl1Mismatch implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;

        public CallableImpl1Mismatch(long experimentId, int threadId, Long min, Long max) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
            } catch (Exception ex) {
                Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public Object call() {
            System.out.println("called threadId: " + threadId);
            final Object lock = new Object();

            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .column("transcriptid")
                    .from("cache", "transcript")
                    .where(eq("key", 1))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(1000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcript")
                        .where(eq("key", 1))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(1000);
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
                    Long tid = row.getLong("transcriptid");

                    Transcript transcript = TRANSCRIPTS.get(tid);

                    BoundStatement bs = TRANSCRIPTALIGNMENT1MISMATCHQUERY.bind().setLong("ei", experimentId).setLong("ti", tid);
                    ResultSet _rs = SESSION.execute(bs);
                    List<Transcriptalignment1mismatch> tal = TRANSCRIPTALIGNMENT1MISMATCHMAPPER.map(_rs).all();

                    TranscriptMappingResults tmr = new TranscriptMappingResults();

                    Stack1Mismatch stack = new Stack1Mismatch();

                    tmr = stack.formatWithCAGEPolyAAndBartelPolyA(tmr, transcript, tal,
                            POLYAALIGNMENTS.get(tid), TSSALIGNMENTS.get(tid), BARTELPOLYAALIGNMENTS.get(tid));

                    Transcriptalignmentsummary tas = new Transcriptalignmentsummary();

                    tas.setExperimentid(experimentId);
                    tas.setTranscriptid(tid);
                    tas.setDistance(1);

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

                    batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.saveQuery(tas, consistencyLevel(ANY)));

                    if (batch.size() > 25) {
//                        SESSION.execute(batch);
                        batch.clear();
                    }

                    if (tid == 2489l) {
                        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("KPNA3_1.html")));
                        bw.write(tmr.mappedAlignments);
                        bw.newLine();
                        bw.flush();
                        bw.close();
                    }

                    if ((count) % 25 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("CreateTranscriptAlignmentSummaries: Created summaries for " + count + " transcripts in experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception e) {
                    Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (batch.size() > 0) {
//                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All transcript summaries in experiment " + experimentId + " on thread " + threadId + " are created, it took " + seconds + " seconds");

            return threadId;
        }
    }

    class CallableImpl2Mismatch implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;

        public CallableImpl2Mismatch(long experimentId, int threadId, Long min, Long max) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
            } catch (Exception ex) {
                Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public Object call() {
            System.out.println("called threadId: " + threadId);
            final Object lock = new Object();

            long start = System.nanoTime();

            Statement stmt = QueryBuilder
                    .select()
                    .column("transcriptid")
                    .from("cache", "transcript")
                    .where(eq("key", 1))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(1000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .from("cache", "transcript")
                        .where(eq("key", 1))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(1000);
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
                    Long tid = row.getLong("transcriptid");

                    Transcript transcript = TRANSCRIPTS.get(tid);

                    BoundStatement bs = TRANSCRIPTALIGNMENT2MISMATCHQUERY.bind().setLong("ei", experimentId).setLong("ti", tid);
                    ResultSet _rs = SESSION.execute(bs);
                    List<Transcriptalignment2mismatch> tal = TRANSCRIPTALIGNMENT2MISMATCHMAPPER.map(_rs).all();

                    TranscriptMappingResults tmr = new TranscriptMappingResults();

                    Stack2Mismatch stack = new Stack2Mismatch();

                    tmr = stack.formatWithCAGEPolyAAndBartelPolyA(tmr, transcript, tal,
                            POLYAALIGNMENTS.get(tid), TSSALIGNMENTS.get(tid), BARTELPOLYAALIGNMENTS.get(tid));

                    Transcriptalignmentsummary tas = new Transcriptalignmentsummary();

                    tas.setExperimentid(experimentId);
                    tas.setTranscriptid(tid);
                    tas.setDistance(2);

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

                    batch.add(TRANSCRIPTALIGNMENTSUMMARYMAPPER.saveQuery(tas, consistencyLevel(ANY)));

                    if (batch.size() > 25) {
//                        SESSION.execute(batch);
                        batch.clear();
                    }

                    if (tid == 2489l) {
                        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("KPNA3_2.html")));
                        bw.write(tmr.mappedAlignments);
                        bw.newLine();
                        bw.flush();
                        bw.close();
                    }

                    if ((count) % 25 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("CreateTranscriptAlignmentSummaries: Created summaries for " + count + " transcripts in experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception e) {
                    Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (batch.size() > 0) {
//                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All transcript summaries in experiment " + experimentId + " on thread " + threadId + " are created, it took " + seconds + " seconds");

            return threadId;
        }
    }

    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            Map<Long, Integer> indexMap = new HashMap<>();
            String line = "";
            System.out.println(args[0]);
            System.out.println(CreateTranscriptAlignmentSummaries.class.getName());
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0] + ", " + tokens[1]);
                indexMap.put(Long.parseLong(tokens[0]), Integer.parseInt(tokens[1]));
            }
            br.close();

            System.out.println("Loading meta alignments..");
            init();
            System.out.println("Submitting display creation..");

            for (final Long key : indexMap.keySet()) {
                Callable callable = (Callable) () -> {
                    CreateTranscriptAlignmentSummaries sstp0 = new CreateTranscriptAlignmentSummaries();
                    sstp0.runThreads(key, indexMap.get(key));
                    return "All alignment summaries in datasets " + key + " are created.";
                };
                CONSUMER_QUEUE.put(callable);
            }
        } catch (IOException | NumberFormatException | InterruptedException ex) {
            Logger.getLogger(CreateTranscriptAlignmentSummaries.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
