package cache.dataimportes;

import cache.dataimportes.util.ConsumerService;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.UPDATETRANSCRIPTALIGNMENT1MISMATCHQUERY;
import static cache.dataimportes.util.DataAccess.UPDATETRANSCRIPTALIGNMENT2MISMATCHQUERY;
import static cache.dataimportes.util.DataAccess.UPDATETRANSCRIPTALIGNMENTQUERY;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class UpdateReadcount {

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    public void updateAlignment0(Long experimentId) {
        try {
            Map<UUID, Long> readCounts = Collections.synchronizedMap(new HashMap<>());
            Statement stmt = QueryBuilder
                    .select()
                    .column("id")
                    .column("count")
                    .from("cache", "reads")
                    .where(eq("experimentid", experimentId))
                    .setFetchSize(100000);
            ResultSet rs = SESSION.execute(stmt);

            int count = 0;
            long start = System.nanoTime();
            while (!rs.isExhausted()) {
                Row row = rs.one();
                readCounts.put(row.getUUID("id"), row.getLong("count"));
                if (count % 1000000 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("UpdateReadcount: Uploaded " + count + " reads from experiment "
                            + experimentId + " in " + seconds1 + " seconds");
                }
                count++;
            }

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

            List<Callable<Object>> callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                Long[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImpl0 t = new CallableImpl0(experimentId, j, range[0], range[1], readCounts);
                callables.add(t);
            }
            System.out.println("line 52..");
            ex.invokeAll(callables);
            ex.shutdown();

            ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                Long[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImpl1 t = new CallableImpl1(experimentId, j, range[0], range[1], readCounts);
                callables.add(t);
            }
            System.out.println("line 52..");
            ex.invokeAll(callables);
            ex.shutdown();

            ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                Long[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImpl2 t = new CallableImpl2(experimentId, j, range[0], range[1], readCounts);
                callables.add(t);
            }
            System.out.println("line 52..");
            ex.invokeAll(callables);
            ex.shutdown();
        } catch (NumberFormatException | InterruptedException e) {
            Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    class CallableImpl0 implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;
        Map<UUID, Long> readCounts;

        public CallableImpl0(long experimentId, int threadId, Long min, Long max, Map<UUID, Long> readCounts) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.readCounts = readCounts;
            } catch (Exception ex) {
                Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
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
                    .column("reversecomplement")
                    .column("id")
                    .column("readid")
                    .from("cache", "transcriptalignment")
                    .where(eq("experimentid", experimentId))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(10000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .column("reversecomplement")
                        .column("id")
                        .column("readid")
                        .from("cache", "transcriptalignment")
                        .where(eq("experimentid", experimentId))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(10000);
            }

            int count = 0;
            start = System.nanoTime();
            BatchStatement batch = new BatchStatement();
            ResultSet rs = SESSION.execute(stmt);

            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    Boolean rc = row.getBool("reversecomplement");
                    UUID id = row.getUUID("id");
                    Long tid = row.getLong("transcriptid");
                    UUID rid = row.getUUID("readid");
                    Long _c = readCounts.get(rid);
                    BoundStatement bs = UPDATETRANSCRIPTALIGNMENTQUERY.bind()
                            .setLong("ei", experimentId)
                            .setLong("ti", tid)
                            .setBool("rc", rc)
                            .setUUID("id", id)
                            .setLong("rdc", _c);
                    batch.add(bs);
                    if (batch.size() > 10000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                    if (count % 100000 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("UpdateReadcount: Updated " + count + " alignments from experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception ex) {
                    Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("Updated " + count + " alignments in experiment " + experimentId + " on thread: " + threadId + " are updated, it took " + seconds + " seconds");

            return threadId;
        }
    }

    class CallableImpl1 implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;
        Map<UUID, Long> readCounts;

        public CallableImpl1(long experimentId, int threadId, Long min, Long max, Map<UUID, Long> readCounts) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.readCounts = readCounts;
            } catch (Exception ex) {
                Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
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
                    .column("reversecomplement")
                    .column("id")
                    .column("mismatchtype")
                    .column("readid")
                    .from("cache", "transcriptalignment1mismatch")
                    .where(eq("experimentid", experimentId))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(10000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .column("reversecomplement")
                        .column("id")
                        .column("mismatchtype")
                        .column("readid")
                        .from("cache", "transcriptalignment1mismatch")
                        .where(eq("experimentid", experimentId))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(10000);
            }

            int count = 0;
            start = System.nanoTime();
            BatchStatement batch = new BatchStatement();
            ResultSet rs = SESSION.execute(stmt);

            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    Boolean rc = row.getBool("reversecomplement");
                    UUID id = row.getUUID("id");
                    String mt = row.getString("mismatchtype");
                    Long tid = row.getLong("transcriptid");
                    UUID rid = row.getUUID("readid");
                    Long _c = readCounts.get(rid);
                    BoundStatement bs = UPDATETRANSCRIPTALIGNMENT1MISMATCHQUERY.bind()
                            .setLong("ei", experimentId)
                            .setLong("ti", tid)
                            .setBool("rc", rc)
                            .setString("mt", mt)
                            .setUUID("id", id)
                            .setLong("rdc", _c);
                    batch.add(bs);
                    if (batch.size() > 10000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                    if (count % 100000 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("UpdateReadcount1: Updated " + count + " alignments from experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception ex) {
                    Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("Updated " + count + " alignments in experiment " + experimentId + " on thread: " + threadId + " are updated, it took " + seconds + " seconds");

            return threadId;
        }
    }

    class CallableImpl2 implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;
        Map<UUID, Long> readCounts;

        public CallableImpl2(long experimentId, int threadId, Long min, Long max, Map<UUID, Long> readCounts) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.readCounts = readCounts;
            } catch (Exception ex) {
                Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
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
                    .column("reversecomplement")
                    .column("id")
                    .column("mismatch1type")
                    .column("mismatch2type")
                    .column("readid")
                    .from("cache", "transcriptalignment2mismatch")
                    .where(eq("experimentid", experimentId))
                    .and(gt("transcriptid", min))
                    .and(lte("transcriptid", max))
                    .setFetchSize(10000);

            if (threadId == 0) {
                stmt = QueryBuilder
                        .select()
                        .column("transcriptid")
                        .column("reversecomplement")
                        .column("id")
                        .column("mismatch1type")
                        .column("mismatch2type")
                        .column("readid")
                        .from("cache", "transcriptalignment2mismatch")
                        .where(eq("experimentid", experimentId))
                        .and(gte("transcriptid", min))
                        .and(lte("transcriptid", max))
                        .setFetchSize(10000);
            }

            int count = 0;
            start = System.nanoTime();
            BatchStatement batch = new BatchStatement();
            ResultSet rs = SESSION.execute(stmt);

            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    Boolean rc = row.getBool("reversecomplement");
                    UUID id = row.getUUID("id");
                    String mt1 = row.getString("mismatch1type");
                    String mt2 = row.getString("mismatch2type");
                    Long tid = row.getLong("transcriptid");
                    UUID rid = row.getUUID("readid");
                    Long _c = readCounts.get(rid);
                    BoundStatement bs = UPDATETRANSCRIPTALIGNMENT2MISMATCHQUERY.bind()
                            .setLong("ei", experimentId)
                            .setLong("ti", tid)
                            .setBool("rc", rc)
                            .setString("mt1", mt1)
                            .setString("mt2", mt2)
                            .setUUID("id", id)
                            .setLong("rdc", _c);
                    batch.add(bs);
                    if (batch.size() > 10000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                    if (count % 100000 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("UpdateReadcount2: Updated " + count + " alignments from experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception ex) {
                    Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("Updated " + count + " alignments in experiment " + experimentId + " on thread: " + threadId + " are updated, it took " + seconds + " seconds");

            return threadId;
        }
    }

    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            List<Long> eids = new ArrayList<>();
            String line = "";
            System.out.println(args[0]);
            System.out.println(UpdateReadcount.class.getName());
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0]);
                eids.add(Long.parseLong(tokens[0]));
            }
            br.close();

            for (final Long eid : eids) {
                Callable callable = (Callable) () -> {
                    UpdateReadcount sstp0 = new UpdateReadcount();
                    sstp0.updateAlignment0(eid);
                    return "All alignments in datasets " + eid + " completed.";
                };
                CONSUMER_QUEUE.put(callable);
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(UpdateReadcount.class.getName()).log(Level.SEVERE, null, ex);
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

}
