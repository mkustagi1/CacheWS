package cache.dataimportes;

import cache.dataimportes.model.Datastatistics;
import cache.dataimportes.util.ConsumerService;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSMAPPER;
import static cache.dataimportes.util.DataAccess.DATASTATISTICSQUERY;
import static cache.dataimportes.util.DataAccess.SESSION;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import com.datastax.driver.core.utils.UUIDs;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class UpdateDatastatistics {

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Logger.getLogger(CreateExperimentSummaries.class.getName()).log(Level.SEVERE, null, ex);
        }
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    public void update(Long experimentId, Integer distance) {
        try {
            Datastatistics ds;

            if (distance == 0) {
                BoundStatement bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", 0);
                ResultSet rs = SESSION.execute(bs);
                ds = DATASTATISTICSMAPPER.map(rs).one();
                if (ds == null) {
                    ds = new Datastatistics();
                    ds.setKey(1);
                    ds.setExperimentid(experimentId);
                    ds.setDistance(0);
                    ds.setId(UUIDs.timeBased());
                    ds.setAnnotations(new HashMap<>());
                }
            } else {
                ds = new Datastatistics();
                ds.setKey(1);
                ds.setExperimentid(experimentId);
                ds.setDistance(distance);
                ds.setId(UUIDs.timeBased());
                ds.setAnnotations(new HashMap<>());
            }

            Statement stmt = null;
            if (null != distance) {
                switch (distance) {
                    case 0:
                        stmt = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .column("reversecomplement")
                                .from("cache", "transcriptalignment")
                                .where(eq("experimentid", experimentId))
                                .setFetchSize(10000);
                        break;
                    case 1:
                        stmt = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .column("reversecomplement")
                                .from("cache", "transcriptalignment1mismatch")
                                .where(eq("experimentid", experimentId))
                                .setFetchSize(10000);
                        break;
                    case 2:
                        stmt = QueryBuilder
                                .select()
                                .column("readid")
                                .column("readcount")
                                .column("reversecomplement")
                                .from("cache", "transcriptalignment2mismatch")
                                .where(eq("experimentid", experimentId))
                                .setFetchSize(10000);
                        break;
                    default:
                        throw new UnsupportedOperationException("Distance: " + distance + " is not supported");
                }
            }

            Map<UUID, Long> rids = new HashMap<>();
            Map<UUID, Long> ridsRC = new HashMap<>();
            ResultSet rs = SESSION.execute(stmt);
            long count = 0l;
            while (!rs.isExhausted()) {
                Row row = rs.one();
                UUID rid = row.getUUID("readid");
                Long rCount = row.getLong("readcount");
                Boolean rc = row.getBool("reversecomplement");
                rids.put(rid, rCount);
                if (rc) {
                    ridsRC.put(rid, rCount);
                }
                if (count % 1000000 == 0) {
                    System.out.println("Traversed " + count + " alignments in experiment " + experimentId + " at distance " + distance + "...");
                }
                count++;
            }

            long nrMapped = rids.size(), totalMapped = rids.values().stream().mapToLong(Number::longValue).sum();
            long nrMappedRC = ridsRC.size(), totalMappedRC = ridsRC.values().stream().mapToLong(Number::longValue).sum();

            if (null != distance) {
                switch (distance) {
                    case 0:
                        break;
                    case 1:
                        BoundStatement bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", 0);
                        ResultSet _rs = SESSION.execute(bs);
                        Datastatistics _ds = DATASTATISTICSMAPPER.map(_rs).one();
                        ds.setTotalreads(_ds.getTotalreads() - _ds.getTotalmappedreads());
                        ds.setTotalreadsnonredundant(_ds.getTotalreadsnonredundant() - _ds.getTotalmappedreadsnonredundant());
                        break;
                    case 2:
                        bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", 1);
                        _rs = SESSION.execute(bs);
                        _ds = DATASTATISTICSMAPPER.map(_rs).one();
                        ds.setTotalreads(_ds.getTotalreads() - _ds.getTotalmappedreads());
                        ds.setTotalreadsnonredundant(_ds.getTotalreadsnonredundant() - _ds.getTotalmappedreadsnonredundant());
                        break;
                    default:
                        throw new UnsupportedOperationException("Distance: " + distance + " is not supported");
                }
            }

            ds.setTotalmappedreads(totalMapped);
            ds.setTotalmappedreadsRC(totalMappedRC);
            ds.setTotalmappedreadsnonredundant(nrMapped);
            ds.setTotalmappedreadsnonredundantRC(nrMappedRC);

            DATASTATISTICSMAPPER.save(ds);

        } catch (Exception e) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public void createMissing(Long experimentId, Integer distance) {
        try {
            BoundStatement bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", distance);
            ResultSet rs = SESSION.execute(bs);
            Datastatistics ds = DATASTATISTICSMAPPER.map(rs).one();
            if (ds == null || ds.getTotalmappedreads() == null) {
                ds = new Datastatistics();
                ds.setKey(1);
                ds.setExperimentid(experimentId);
                ds.setDistance(distance);
                ds.setId(UUIDs.timeBased());
                ds.setAnnotations(new HashMap<>());

                Statement stmt = null;
                if (null != distance) {
                    switch (distance) {
                        case 0:
                            stmt = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .column("reversecomplement")
                                    .from("cache", "transcriptalignment")
                                    .where(eq("experimentid", experimentId))
                                    .setFetchSize(10000);
                            break;
                        case 1:
                            stmt = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .column("reversecomplement")
                                    .from("cache", "transcriptalignment1mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .setFetchSize(10000);
                            break;
                        case 2:
                            stmt = QueryBuilder
                                    .select()
                                    .column("readid")
                                    .column("readcount")
                                    .column("reversecomplement")
                                    .from("cache", "transcriptalignment2mismatch")
                                    .where(eq("experimentid", experimentId))
                                    .setFetchSize(10000);
                            break;
                        default:
                            throw new UnsupportedOperationException("Distance: " + distance + " is not supported");
                    }
                }

                Map<UUID, Long> rids = new HashMap<>();
                Map<UUID, Long> ridsRC = new HashMap<>();
                rs = SESSION.execute(stmt);
                long count = 0l;
                while (!rs.isExhausted()) {
                    Row row = rs.one();
                    UUID rid = row.getUUID("readid");
                    Long rCount = row.getLong("readcount");
                    Boolean rc = row.getBool("reversecomplement");
                    rids.put(rid, rCount);
                    if (rc) {
                        ridsRC.put(rid, rCount);
                    }
                    if (count % 1000000 == 0) {
                        System.out.println("Traversed " + count + " alignments in experiment " + experimentId + " at distance " + distance + "...");
                    }
                    count++;
                }

                long nrMapped = rids.size(), totalMapped = rids.values().stream().mapToLong(Number::longValue).sum();
                long nrMappedRC = ridsRC.size(), totalMappedRC = ridsRC.values().stream().mapToLong(Number::longValue).sum();
                if (null != distance) {
                    switch (distance) {
                        case 0:
                            stmt = QueryBuilder
                                    .select()
                                    .countAll()
                                    .from("cache", "reads")
                                    .where(eq("experimentid", experimentId));
                            Long _nrc = SESSION.execute(stmt).one().getLong(0);
                            ds.setTotalreadsnonredundant(_nrc);
                            stmt = QueryBuilder
                                    .select()
                                    .fcall("sum", QueryBuilder.column("count"))
                                    .from("cache", "reads")
                                    .where(eq("experimentid", experimentId));
                            Long _tc = SESSION.execute(stmt).one().getLong(0);
                            ds.setTotalreads(_tc);
                            break;
                        case 1:
                            bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", 0);
                            ResultSet _rs = SESSION.execute(bs);
                            Datastatistics _ds = DATASTATISTICSMAPPER.map(_rs).one();
                            ds.setTotalreads(_ds.getTotalreads() - _ds.getTotalmappedreads());
                            ds.setTotalreadsnonredundant(_ds.getTotalreadsnonredundant() - _ds.getTotalmappedreadsnonredundant());
                            break;
                        case 2:
                            bs = DATASTATISTICSQUERY.bind().setInt("ky", 1).setLong("eid", experimentId).setInt("d", 1);
                            _rs = SESSION.execute(bs);
                            _ds = DATASTATISTICSMAPPER.map(_rs).one();
                            ds.setTotalreads(_ds.getTotalreads() - _ds.getTotalmappedreads());
                            ds.setTotalreadsnonredundant(_ds.getTotalreadsnonredundant() - _ds.getTotalmappedreadsnonredundant());
                            break;
                        default:
                            throw new UnsupportedOperationException("Distance: " + distance + " is not supported");
                    }
                }

                ds.setTotalmappedreads(totalMapped);
                ds.setTotalmappedreadsRC(totalMappedRC);
                ds.setTotalmappedreadsnonredundant(nrMapped);
                ds.setTotalmappedreadsnonredundantRC(nrMappedRC);

                DATASTATISTICSMAPPER.save(ds);
            }

        } catch (Exception e) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            List<Long> eids = new ArrayList<>();
            String line = "";
            System.out.println(args[0]);
            System.out.println(UpdateDatastatistics.class.getName());
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0] + ", " + tokens[1]);
                if (!eids.contains(Long.parseLong(tokens[0]))) {
                    eids.add(Long.parseLong(tokens[0]));
                }
            }
            br.close();

            List<Callable<Object>> callables0 = new ArrayList<>();
            List<Callable<Object>> callables1 = new ArrayList<>();
            List<Callable<Object>> callables2 = new ArrayList<>();
            for (final Long key : eids) {
                Callable callable = (Callable) () -> {
                    UpdateDatastatistics sstp0 = new UpdateDatastatistics();
                    sstp0.createMissing(key, 0);
                    return "All alignments in datasets " + key + " completed.";
                };
                callables0.add(callable);

                callable = (Callable) () -> {
                    UpdateDatastatistics sstp0 = new UpdateDatastatistics();
                    sstp0.createMissing(key, 1);
                    return "All alignments in datasets " + key + " completed.";
                };
                callables1.add(callable);
                callable = (Callable) () -> {
                    UpdateDatastatistics sstp0 = new UpdateDatastatistics();
                    sstp0.createMissing(key, 2);
                    return "All alignments in datasets " + key + " completed.";
                };
                callables2.add(callable);

            }

            System.out.println("Invoking callables 0");
            Callable _c0 = (Callable) () -> {
                ExecutorService ex = Executors.newFixedThreadPool(20, (Runnable r) -> {
                    Thread t = new Thread(r);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });
                ex.invokeAll(callables0)
                        .stream()
                        .map(future -> {
                            try {
                                return future.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new IllegalStateException(e);
                            }
                        })
                        .forEach(System.out::println);
                ex.shutdown();
                return "Done 0";
            };
            CONSUMER_QUEUE.put(_c0);

            System.out.println("Invoking callables 1");
            Callable _c1 = (Callable) () -> {
                ExecutorService ex = Executors.newFixedThreadPool(20, (Runnable r) -> {
                    Thread t = new Thread(r);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });
                ex.invokeAll(callables1)
                        .stream()
                        .map(future -> {
                            try {
                                return future.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new IllegalStateException(e);
                            }
                        })
                        .forEach(System.out::println);
                ex.shutdown();
                return "Done 1";
            };
            CONSUMER_QUEUE.put(_c1);

            System.out.println("Invoking callables 2");
            Callable _c2 = (Callable) () -> {
                ExecutorService ex = Executors.newFixedThreadPool(20, (Runnable r) -> {
                    Thread t = new Thread(r);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });
                ex.invokeAll(callables2)
                        .stream()
                        .map(future -> {
                            try {
                                return future.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new IllegalStateException(e);
                            }
                        })
                        .forEach(System.out::println);
                ex.shutdown();
                return "Done 2";
            };
            CONSUMER_QUEUE.put(_c2);
        } catch (IOException | NumberFormatException | InterruptedException ex) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
