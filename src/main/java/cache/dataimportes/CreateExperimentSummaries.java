package cache.dataimportes;

import cache.dataimportes.holders.SearchResult;
import cache.dataimportes.holders.Strand;
import static cache.dataimportes.holders.Strand.BOTH;
import static cache.dataimportes.holders.Strand.FORWARD;
import static cache.dataimportes.holders.Strand.REVERSECOMPLEMENT;
import cache.dataimportes.model.Datastatistics;
import cache.dataimportes.model.Experiment;
import cache.dataimportes.model.Experimentsummary;
import cache.dataimportes.model.Transcript;
import cache.dataimportes.util.ConsumerService;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSMAPPER;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTMAPPER;
import static cache.dataimportes.util.DataAccess.EXPERIMENTQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTSUMMARYCOUNTQUERY;
import static cache.dataimportes.util.DataAccess.EXPERIMENTSUMMARYMAPPER;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTMAPPER;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
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
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import com.datastax.driver.core.utils.UUIDs;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.collections4.CollectionUtils;

public class CreateExperimentSummaries {

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static final Map<Long, Transcript> TRANSCRIPTS = new HashMap<>();
    static Integer NUMTHREADS = 100;
    static Map<Integer, Long[]> BOUNDS = new HashMap<>();

    static {
        LogManager.getLogManager().reset();
        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.WARNING);
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

                if (count % 1000 == 0) {
                    long end1 = System.nanoTime();
                    long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                    System.out.println("CreateExperimentSummaries: Loaded transcripts for " + count + " transcripts in " + seconds1 + " seconds");
                }
                count++;
            } catch (Exception e) {
                Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, e);
            }
        }

        stmt = QueryBuilder
                .select()
                .countAll()
                .from("cache", "transcript")
                .where(eq("key", 1));

        rs = SESSION.execute(stmt);
        Long rowCount = rs.one().getLong(0);
        System.out.println("rowCount: " + rowCount);

        Long batchSize = (long) Math.ceil(rowCount.doubleValue() / NUMTHREADS.doubleValue());
        System.out.println("batchSize: " + batchSize);

        stmt = QueryBuilder
                .select()
                .column("transcriptid")
                .from("cache", "transcript")
                .where(eq("key", 1))
                .setFetchSize(2000);
        rs = SESSION.execute(stmt);

        int i = 0, tCount = 0;
        Long min = null, max = null;
        while (!rs.isExhausted()) {
            Row row = rs.one();

            if (i == 0) {
                min = row.getLong("transcriptid");
            } else if (i == (tCount + 1) * batchSize) {
                max = row.getLong("transcriptid");
                BOUNDS.put(tCount, new Long[]{min, max});
                tCount++;
                min = max;
            } else if ((i == rowCount - 1) && (BOUNDS.get(NUMTHREADS - 1) == null)) {
                max = row.getLong("transcriptid");
                BOUNDS.put((NUMTHREADS - 1), new Long[]{min, max});
            }

            i++;
        }
    }

    public static long[] getMappedReadCounts(long experimentId, int distance, long tid, Strand strand) {
        long nr = 0, tr = 0;

        switch (distance) {
            case 0:
                Statement stmt1;
                Map<UUID, Long> mappedReads = Collections.synchronizedMap(new HashMap<>());
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            case 1:
                mappedReads = Collections.synchronizedMap(new HashMap<>());
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            case 2:
                mappedReads = Collections.synchronizedMap(new HashMap<>());
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            default:
                break;
        }
        return new long[]{nr, tr};
    }

    public static long[] computeDeltaReadCount(long experimentId, long oldTranscriptId, List<SearchResult> srs) {
        long delta = 0, deltanr = 0, deltarc = 0, deltanrrc = 0;

        List<UUID> newList = new ArrayList<>();
        List<UUID> oldList = new ArrayList<>();
        srs.stream().forEach((sr) -> {
            newList.add(sr.readID);
        });

        Map<UUID, Long> mappedReads = Collections.synchronizedMap(new HashMap<>());
        Map<UUID, Boolean> strand = Collections.synchronizedMap(new HashMap<>());
        Statement stmt1 = QueryBuilder
                .select()
                .column("readid")
                .column("readcount")
                .column("reversecomplement")
                .from("cache", "transcriptalignment")
                .where(eq("experimentid", experimentId))
                .and(eq("transcriptid", oldTranscriptId))
                .setFetchSize(1000);

        ResultSet _rs = SESSION.execute(stmt1);
        while (!_rs.isExhausted()) {
            Row _row = _rs.one();
            UUID rid = _row.getUUID("readid");
            mappedReads.put(rid, _row.getLong("readcount"));
            strand.put(rid, _row.getBool("reversecomplement"));
            oldList.add(rid);
        }

        Collection<UUID> intersection = CollectionUtils.intersection(newList, oldList);
        Collection<UUID> plus = CollectionUtils.subtract(newList, intersection);
        Collection<UUID> minus = CollectionUtils.subtract(oldList, intersection);

        for (UUID uid : plus) {
            long rc = mappedReads.get(uid);
            boolean s = strand.get(uid);
            delta += rc;
            deltanr++;
            if (s) {
                deltarc += rc;
                deltanrrc++;
            }
        }

        for (UUID uid : minus) {
            long rc = mappedReads.get(uid);
            boolean s = strand.get(uid);
            delta -= rc;
            deltanr--;
            if (s) {
                deltarc -= rc;
                deltanrrc--;
            }
        }

        return new long[]{delta, deltanr, deltarc, deltanrrc};
    }

    public static long[] getMappedReadCountsinTranscripts(long experimentId, int distance, List<Long> tids, Strand strand) {
        long nr = 0, tr = 0;

        switch (distance) {
            case 0:
                Statement stmt1;
                Map<UUID, Long> mappedReads = Collections.synchronizedMap(new HashMap<>());
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            case 1:
                mappedReads = Collections.synchronizedMap(new HashMap<>());
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            case 2:
                mappedReads = Collections.synchronizedMap(new HashMap<>());
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(in("transcriptid", tids))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            default:
                break;
        }
        return new long[]{nr, tr};
    }

    public static long[] getMappedReadCountsAndTranscriptsMapped(long experimentId, int distance, long tid, Strand strand) {
        long nr = 0, tr = 0, ot = 0;
        try {
        Map<UUID, Long> mappedReads = Collections.synchronizedMap(new HashMap<>());

        switch (distance) {
            case 0:
                Statement stmt1;
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            case 1:
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            case 2:
                switch (strand) {
                    case FORWARD: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.FALSE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case REVERSECOMPLEMENT: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .and(eq("reversecomplement", Boolean.TRUE))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    case BOTH: {
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                    }
                    default:
                        stmt1 = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .and(eq("transcriptid", tid))
                                .setFetchSize(1000);

                        ResultSet _rs = SESSION.execute(stmt1);
                        while (!_rs.isExhausted()) {
                            Row _row = _rs.one();
                            mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                        }
                        nr = mappedReads.size();
                        tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();
                        break;
                }
                break;
            default:
                break;
        }

        Set<Long> mappedTranscripts = Collections.synchronizedSet(new HashSet<>());

        final List<UUID> list = new ArrayList<>(mappedReads.keySet());
        final int listSize = list.size(),
                chunkSize = 1000;
        List<List<UUID>> list2
                = IntStream.range(0, (listSize - 1) / chunkSize + 1)
                .mapToObj(i -> list.subList(i *= chunkSize,
                        listSize - chunkSize >= i ? i + chunkSize : listSize))
                .collect(Collectors.toList());

        Spliterator<List<UUID>> k6 = list2.spliterator();
        Consumer<List<UUID>> actions = (List<UUID> _l) -> {
            Set<Long> _set = new HashSet<>();
            Statement stmt2 = QueryBuilder
                    .select()
                    .column("transcriptid")
                    .from("cache", "transcriptalignmenttranspose")
                    .where(eq("experimentid", experimentId))
                    .and(in("readid", _l.toArray()))
                    .setFetchSize(10000);

            ResultSet _rs1 = SESSION.execute(stmt2);
            while (!_rs1.isExhausted()) {
                Row _row = _rs1.one();
                _set.add(_row.getLong("transcriptid"));
            }
            mappedTranscripts.addAll(_set);
        };
        long targetBatchSize = k6.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
        new ParEach(null, k6, actions, targetBatchSize).invoke();
        ot = mappedTranscripts.size();
        } catch (Exception ex) {
            Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, ex);
        }

        return new long[]{nr, tr, ot};
    }

    public void runThreads(Long experimentId, Integer distance) {
        try {
            System.out.println("line 111..");

            BoundStatement bs = EXPERIMENTQUERY.bind().setInt("ky", 1).setLong("id", experimentId).setInt("d", distance);
            ResultSet _rs = SESSION.execute(bs);
            Experiment experiment = EXPERIMENTMAPPER.map(_rs).one();

            bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", distance);
            _rs = SESSION.execute(bs);
            Datastatistics ds = DATASTATISTICSMAPPER.map(_rs).one();

            System.out.println("experimentid, distance: " + experimentId + ", " + distance);
            String annotation = experiment.getAnnotation();
            Strand strand;

            if (annotation.contains("directional")) {
                if (annotation.endsWith("_FORWARD")) {
                    strand = Strand.FORWARD;
                } else {
                    strand = Strand.REVERSECOMPLEMENT;
                }
            } else {
                strand = Strand.BOTH;
            }

            switch (ds.getDistance()) {
                case 0:
                    ExecutorService ex = Executors.newFixedThreadPool(NUMTHREADS, (Runnable r) -> {
                        Thread t = new Thread(r);
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    });
                    System.out.println("ExecutorService created..");

                    List<Callable<Object>> callables = new ArrayList<>();
                    for (int j = 0; j < NUMTHREADS; j++) {
                        Long[] range = BOUNDS.get(j);
                        System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                        CallableImplExactMatch t = new CallableImplExactMatch(experimentId, j, range[0], range[1], strand, ds);
                        callables.add(t);
                    }

                    System.out.println("Invoking callables");
                    ex.invokeAll(callables);
                    ex.shutdown();
                    break;
                case 1:
                    ex = Executors.newFixedThreadPool(NUMTHREADS, (Runnable r) -> {
                        Thread t = new Thread(r);
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    });
                    System.out.println("ExecutorService created..");

                    callables = new ArrayList<>();
                    for (int j = 0; j < NUMTHREADS; j++) {
                        Long[] range = BOUNDS.get(j);
                        System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                        CallableImpl1Mismatch t = new CallableImpl1Mismatch(experimentId, j, range[0], range[1], strand, ds);
                        callables.add(t);
                    }

                    System.out.println("Invoking callables");
                    ex.invokeAll(callables);
                    ex.shutdown();
                    break;
                case 2:
                    ex = Executors.newFixedThreadPool(NUMTHREADS, (Runnable r) -> {
                        Thread t = new Thread(r);
                        t.setPriority(Thread.MAX_PRIORITY);
                        return t;
                    });
                    System.out.println("ExecutorService created..");

                    callables = new ArrayList<>();
                    for (int j = 0; j < NUMTHREADS; j++) {
                        Long[] range = BOUNDS.get(j);
                        System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                        CallableImpl2Mismatch t = new CallableImpl2Mismatch(experimentId, j, range[0], range[1], strand, ds);
                        callables.add(t);
                    }

                    System.out.println("Invoking callables");
                    ex.invokeAll(callables);
                    ex.shutdown();
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    class CallableImplExactMatch implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;
        Strand strand;
        Datastatistics datastatistics;

        public CallableImplExactMatch(long experimentId, int threadId, Long min, Long max, Strand strand, Datastatistics ds) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.strand = strand;
                this.datastatistics = ds;
            } catch (Exception ex) {
                Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, ex);
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

            ResultSet rs = SESSION.execute(stmt);

            int count = 0;
            BatchStatement batch = new BatchStatement();
            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    Long tid = row.getLong("transcriptid");

                    Transcript transcript = TRANSCRIPTS.get(tid);

                    BoundStatement bs = EXPERIMENTSUMMARYCOUNTQUERY.bind().setLong("eid", experimentId).setInt("d", 0).setLong("ti", tid);
                    Long _c = SESSION.execute(bs).one().getLong(0);
                    if (_c > 0) {
                        continue;
                    }

                    Statement stmt1;
                    Map<UUID, Long> mappedReads = Collections.synchronizedMap(new HashMap<>());
                    switch (strand) {
                        case FORWARD: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .and(eq("reversecomplement", Boolean.FALSE))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        case REVERSECOMPLEMENT: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .and(eq("reversecomplement", Boolean.TRUE))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        case BOTH: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        default:
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                    }

                    Experimentsummary es = new Experimentsummary();

                    es.setExperimentid(experimentId);
                    es.setDistance(datastatistics.getDistance());
                    es.setTranscriptid(transcript.getTranscriptid());
                    es.setSymbol(transcript.getName());
                    es.setId(UUIDs.timeBased());
                    es.setTranscriptlength(transcript.getSequence().length());

                    long nr = mappedReads.size(), tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();

                    Set<Long> mappedTranscripts = Collections.synchronizedSet(new HashSet<>());

                    final List<UUID> list = new ArrayList<>(mappedReads.keySet());
                    final int listSize = list.size(), chunkSize = 1000;
                    List<List<UUID>> list2
                            = IntStream.range(0, (listSize - 1) / chunkSize + 1)
                            .mapToObj(i -> list.subList(i *= chunkSize,
                                    listSize - chunkSize >= i ? i + chunkSize : listSize))
                            .collect(Collectors.toList());

                    Spliterator<List<UUID>> k6 = list2.spliterator();
                    Consumer<List<UUID>> actions = (List<UUID> _l) -> {
                        Set<Long> _set = new HashSet<>();
                        Statement stmt2 = QueryBuilder
                                .select()
                                .column("transcriptid")
                                .from("cache", "transcriptalignmenttranspose")
                                .where(eq("experimentid", experimentId))
                                .and(in("readid", _l.toArray()))
                                .setFetchSize(10000);

                        ResultSet _rs1 = SESSION.execute(stmt2);
                        while (!_rs1.isExhausted()) {
                            Row _row = _rs1.one();
                            _set.add(_row.getLong("transcriptid"));
                        }
                        mappedTranscripts.addAll(_set);
                    };
                    long targetBatchSize = k6.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
                    new ParEach(null, k6, actions, targetBatchSize).invoke();

                    es.setNonredundantreads(nr);
                    es.setTotalreads(tr);
                    es.setTranscriptsmappedbyreads(mappedTranscripts.size());
                    es.setMappedreadsbylength(1000d * (double) tr / (double) es.getTranscriptlength());

                    switch (strand) {
                        case FORWARD:
                            long fc = datastatistics.getTotalmappedreads() - datastatistics.getTotalmappedreadsRC();
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / fc);
                            break;
                        case REVERSECOMPLEMENT:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreadsRC());
                            break;
                        case BOTH:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreads());
                            break;
                        default:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreads());
                            break;
                    }

                    batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(es));
                    if (batch.size() > 100) {
                        SESSION.execute(batch);
                        batch.clear();
                    }

                    if ((count) % 100 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("CreateExperimentSummaries: Created summaries for " + count + " transcripts in experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception e) {
                    Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All transcript summaries in experiment0 " + experimentId + " on thread " + threadId + " are created, it took " + seconds + " seconds");

            return threadId;
        }
    }

    class CallableImpl1Mismatch implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;
        Strand strand;
        Datastatistics datastatistics;

        public CallableImpl1Mismatch(long experimentId, int threadId, Long min, Long max, Strand strand, Datastatistics ds) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.strand = strand;
                this.datastatistics = ds;
            } catch (Exception ex) {
                Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, ex);
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

            ResultSet rs = SESSION.execute(stmt);

            int count = 0;
            BatchStatement batch = new BatchStatement();
            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    Long tid = row.getLong("transcriptid");

                    Transcript transcript = TRANSCRIPTS.get(tid);

                    BoundStatement bs = EXPERIMENTSUMMARYCOUNTQUERY.bind().setLong("eid", experimentId).setInt("d", 1).setLong("ti", tid);
                    Long _c = SESSION.execute(bs).one().getLong(0);
                    if (_c > 0) {
                        continue;
                    }

                    Statement stmt1;
                    Map<UUID, Long> mappedReads = Collections.synchronizedMap(new HashMap<>());
                    switch (strand) {
                        case FORWARD: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment1mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .and(eq("reversecomplement", Boolean.FALSE))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        case REVERSECOMPLEMENT: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment1mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .and(eq("reversecomplement", Boolean.TRUE))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        case BOTH: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment1mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        default:
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment1mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                    }

                    Experimentsummary es = new Experimentsummary();

                    es.setExperimentid(experimentId);
                    es.setDistance(datastatistics.getDistance());
                    es.setTranscriptid(transcript.getTranscriptid());
                    es.setSymbol(transcript.getName());
                    es.setId(UUIDs.timeBased());
                    es.setTranscriptlength(transcript.getSequence().length());

                    long nr = mappedReads.size(), tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();

                    Set<Long> mappedTranscripts = Collections.synchronizedSet(new HashSet<>());

                    final List<UUID> list = new ArrayList<>(mappedReads.keySet());
                    final int listSize = list.size(), chunkSize = 1000;
                    List<List<UUID>> list2
                            = IntStream.range(0, (listSize - 1) / chunkSize + 1)
                            .mapToObj(i -> list.subList(i *= chunkSize,
                                    listSize - chunkSize >= i ? i + chunkSize : listSize))
                            .collect(Collectors.toList());

                    Spliterator<List<UUID>> k6 = list2.spliterator();
                    Consumer<List<UUID>> actions = (List<UUID> _l) -> {
                        Set<Long> _set = new HashSet<>();
                        Statement stmt2 = QueryBuilder
                                .select()
                                .column("transcriptid")
                                .from("cache", "transcriptalignmenttranspose")
                                .where(eq("experimentid", experimentId))
                                .and(in("readid", _l.toArray()))
                                .setFetchSize(10000);

                        ResultSet _rs1 = SESSION.execute(stmt2);
                        while (!_rs1.isExhausted()) {
                            Row _row = _rs1.one();
                            _set.add(_row.getLong("transcriptid"));
                        }
                        mappedTranscripts.addAll(_set);
                    };
                    long targetBatchSize = k6.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
                    new ParEach(null, k6, actions, targetBatchSize).invoke();

                    es.setNonredundantreads(nr);
                    es.setTotalreads(tr);
                    es.setTranscriptsmappedbyreads(mappedTranscripts.size());
                    es.setMappedreadsbylength(1000d * (double) tr / (double) es.getTranscriptlength());

                    switch (strand) {
                        case FORWARD:
                            long fc = datastatistics.getTotalmappedreads() - datastatistics.getTotalmappedreadsRC();
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / fc);
                            break;
                        case REVERSECOMPLEMENT:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreadsRC());
                            break;
                        case BOTH:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreads());
                            break;
                        default:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreads());
                            break;
                    }

                    batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(es));
                    if (batch.size() > 100) {
                        SESSION.execute(batch);
                        batch.clear();
                    }

                    if ((count) % 100 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("CreateExperimentSummaries: Created summaries for " + count + " transcripts in experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception e) {
                    Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All transcript summaries in experiment1 " + experimentId + " on thread " + threadId + " are created, it took " + seconds + " seconds");

            return threadId;
        }
    }

    class CallableImpl2Mismatch implements Callable {

        Long experimentId;
        Long min;
        Long max;
        int threadId;
        Strand strand;
        Datastatistics datastatistics;

        public CallableImpl2Mismatch(long experimentId, int threadId, Long min, Long max, Strand strand, Datastatistics ds) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.strand = strand;
                this.datastatistics = ds;
            } catch (Exception ex) {
                Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, ex);
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

            ResultSet rs = SESSION.execute(stmt);

            int count = 0;
            BatchStatement batch = new BatchStatement();
            while (!rs.isExhausted()) {
                try {
                    Row row = rs.one();
                    Long tid = row.getLong("transcriptid");

                    Transcript transcript = TRANSCRIPTS.get(tid);

                    BoundStatement bs = EXPERIMENTSUMMARYCOUNTQUERY.bind().setLong("eid", experimentId).setInt("d", 2).setLong("ti", tid);
                    Long _c = SESSION.execute(bs).one().getLong(0);
                    if (_c > 0) {
                        continue;
                    }

                    Statement stmt1;
                    Map<UUID, Long> mappedReads = Collections.synchronizedMap(new HashMap<>());
                    switch (strand) {
                        case FORWARD: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment2mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .and(eq("reversecomplement", Boolean.FALSE))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        case REVERSECOMPLEMENT: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment2mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .and(eq("reversecomplement", Boolean.TRUE))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        case BOTH: {
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment2mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                        }
                        default:
                            stmt1 = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .from("cache", "transcriptalignment2mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .and(eq("transcriptid", tid))
                                    .setFetchSize(1000);

                            ResultSet _rs = SESSION.execute(stmt1);
                            while (!_rs.isExhausted()) {
                                Row _row = _rs.one();
                                mappedReads.put(_row.getUUID("readid"), _row.getLong("readcount"));
                            }
                            break;
                    }

                    Experimentsummary es = new Experimentsummary();

                    es.setExperimentid(experimentId);
                    es.setDistance(datastatistics.getDistance());
                    es.setTranscriptid(transcript.getTranscriptid());
                    es.setSymbol(transcript.getName());
                    es.setId(UUIDs.timeBased());
                    es.setTranscriptlength(transcript.getSequence().length());

                    long nr = mappedReads.size(), tr = mappedReads.values().stream().mapToLong(Number::longValue).sum();

                    Set<Long> mappedTranscripts = Collections.synchronizedSet(new HashSet<>());

                    final List<UUID> list = new ArrayList<>(mappedReads.keySet());
                    final int listSize = list.size(), chunkSize = 1000;
                    List<List<UUID>> list2
                            = IntStream.range(0, (listSize - 1) / chunkSize + 1)
                            .mapToObj(i -> list.subList(i *= chunkSize,
                                    listSize - chunkSize >= i ? i + chunkSize : listSize))
                            .collect(Collectors.toList());

                    Spliterator<List<UUID>> k6 = list2.spliterator();
                    Consumer<List<UUID>> actions = (List<UUID> _l) -> {
                        Set<Long> _set = new HashSet<>();
                        Statement stmt2 = QueryBuilder
                                .select()
                                .column("transcriptid")
                                .from("cache", "transcriptalignmenttranspose")
                                .where(eq("experimentid", experimentId))
                                .and(in("readid", _l.toArray()))
                                .setFetchSize(10000);

                        ResultSet _rs1 = SESSION.execute(stmt2);
                        while (!_rs1.isExhausted()) {
                            Row _row = _rs1.one();
                            _set.add(_row.getLong("transcriptid"));
                        }
                        mappedTranscripts.addAll(_set);
                    };
                    long targetBatchSize = k6.estimateSize() / (ForkJoinPool.getCommonPoolParallelism() * 10);
                    new ParEach(null, k6, actions, targetBatchSize).invoke();

                    es.setNonredundantreads(nr);
                    es.setTotalreads(tr);
                    es.setTranscriptsmappedbyreads(mappedTranscripts.size());
                    es.setMappedreadsbylength(1000d * (double) tr / (double) es.getTranscriptlength());

                    switch (strand) {
                        case FORWARD:
                            long fc = datastatistics.getTotalmappedreads() - datastatistics.getTotalmappedreadsRC();
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / fc);
                            break;
                        case REVERSECOMPLEMENT:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreadsRC());
                            break;
                        case BOTH:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreads());
                            break;
                        default:
                            es.setRpkm(1000000d * es.getMappedreadsbylength() / datastatistics.getTotalmappedreads());
                            break;
                    }

                    batch.add(EXPERIMENTSUMMARYMAPPER.saveQuery(es));
                    if (batch.size() > 100) {
                        SESSION.execute(batch);
                        batch.clear();
                    }

                    if ((count) % 100 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("CreateExperimentSummaries: Created summaries for " + count + " transcripts in experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception e) {
                    Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All transcript summaries in experiment2 " + experimentId + " on thread " + threadId + " are created, it took " + seconds + " seconds");

            return threadId;
        }
    }

    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            List<Pair> indexMap = new ArrayList<>();
            String line = "";
            System.out.println(args[0]);
            System.out.println(CreateExperimentSummaries.class.getName());
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0] + ", " + tokens[1]);
                indexMap.add(new Pair(Long.parseLong(tokens[0]), Integer.parseInt(tokens[1])));
            }
            br.close();

            System.out.println("Loading transcript..");
            init();
            System.out.println("Submitting summary creation..");

            for (Pair key : indexMap) {
                Callable callable = (Callable) () -> {
                    CreateExperimentSummaries sstp0 = new CreateExperimentSummaries();
                    sstp0.runThreads(key.eid, key.distance);
                    return "All experiment summaries in datasets " + key + " are created.";
                };
                CONSUMER_QUEUE.add(callable);
            }
        } catch (IOException | NumberFormatException ex) {
            Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static class Pair {

        long eid;
        int distance;

        Pair(Long l, int d) {
            eid = l;
            distance = d;
        }
    }

    static class ParEach<T> extends CountedCompleter<Void> {

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
