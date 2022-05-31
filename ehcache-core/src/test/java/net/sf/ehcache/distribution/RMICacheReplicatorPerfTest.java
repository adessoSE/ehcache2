package net.sf.ehcache.distribution;

import net.sf.ehcache.AbstractCacheTest;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.StopWatch;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.ConfigurationFactory;
import net.sf.ehcache.management.ManagementService;
import net.sf.ehcache.util.RetryAssert;
import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Alex Snaps
 */
public class RMICacheReplicatorPerfTest extends AbstractRMITest {

    private static final Logger LOG = LoggerFactory.getLogger(RMICacheReplicatorPerfTest.class);

    private static final String ASYNCHRONOUS_CACHE = "asynchronousCache";
    private static final String SYNCHRONOUS_CACHE = "synchronousCache";
    private static final String VALUE = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    private static List<CacheManager> createCluster(int size, String... caches) {
        LOG.info("Creating Cluster");
        Collection<String> required = Arrays.asList(caches);
        List<Configuration> configurations = new ArrayList<>(size);
        for (int i = 1; i <= size; i++) {
            Configuration config = ConfigurationFactory.parseConfiguration(RMICacheReplicatorPerfTest.class.getResource("/ehcache-perf-distributed.xml")).name("cm" + i);
            if (!required.isEmpty()) {
                config.getCacheConfigurations().entrySet().removeIf(stringCacheConfigurationEntry -> !required.contains(stringCacheConfigurationEntry.getKey()));
            }
            configurations.add(config);
        }
        LOG.info("Created Configurations");

        List<CacheManager> members = startupManagers(configurations);
        try {
            LOG.info("Created Managers");
            if (required.isEmpty()) {
                waitForClusterMembership(120, TimeUnit.SECONDS, members);
                LOG.info("Cluster Membership Complete");
                emptyCaches(120, TimeUnit.SECONDS, members);
                LOG.info("Caches Emptied");
            } else {
                waitForClusterMembership(120, TimeUnit.SECONDS, required, members);
                LOG.info("Cluster Membership Complete");
                emptyCaches(120, TimeUnit.SECONDS, required, members);
                LOG.info("Caches Emptied");
            }
            return members;
        } finally {
            destroyCluster(members);
        }
    }

    private static void destroyCluster(List<CacheManager> members) {
        members.forEach(CacheManager::shutdown);
    }

    @Before
    public void setUp() throws Exception {
        MulticastKeepaliveHeartbeatSender.setHeartBeatInterval(1000);

        assertThat(getActiveReplicationThreads()).isEmpty();
    }

    @After
    public void noReplicationThreads() {
        RetryAssert.assertBy(30, TimeUnit.SECONDS, AbstractRMITest::getActiveReplicationThreads, IsEmptyCollection.empty());
    }

    /**
     * Performance and capacity tests.
     * <p>
     * The numbers given are for the remote peer tester (java -jar ehcache-1.x-remote-debugger.jar ehcache-distributed1.xml)
     * running on a 10Mbit ethernet network and are measured from the time the peer starts receiving to when
     * it has fully received.
     * <p>
     * r37 and earlier - initial implementation
     * 38 seconds to get all notifications with 6 peers, 2000 Elements and 400 byte payload
     * 18 seconds to get all notifications with 2 peers, 2000 Elements and 400 byte payload
     * 40 seconds to get all notifications with 2 peers, 2000 Elements and 10k payload
     * 22 seconds to get all notifications with 2 peers, 2000 Elements and 1k payload
     * 26 seconds to get all notifications with 2 peers, 200 Elements and 100k payload
     * <p>
     * r38 - RMI stub lookup on registration rather than at each lookup. Saves quite a few lookups. Also change to 5 second heartbeat
     * 38 seconds to get 2000 notifications with 6 peers, Elements with 400 byte payload (1 second heartbeat)
     * 16 seconds to get 2000 notifications with 6 peers, Elements with 400 byte payload (5 second heartbeat)
     * 13 seconds to get 2000 notifications with 2 peers, Elements with 400 byte payload
     * <p>
     * r39 - Batching asyn replicator. Send all queued messages in one RMI call once per second.
     * 2 seconds to get 2000 notifications with 6 peers, Elements with 400 byte payload (5 second heartbeat)
     */
    @Test
    public void testBigPutsProgagatesAsynchronous() throws CacheException {
        List<CacheManager> cluster = createCluster(5, ASYNCHRONOUS_CACHE);
        try {
            final Ehcache cache = cluster.get(0).getEhcache(ASYNCHRONOUS_CACHE);

            StopWatch stopWatch = new StopWatch();
            setUpCache(2, cache);
            long elapsed = stopWatch.getElapsedTime();
            long putTime = ((elapsed / 1000));
            LOG.info("Put Elapsed time: " + putTime);
            //assertTrue(putTime < 8);

            for (CacheManager manager : cluster) {
                RetryAssert.assertBy(2, TimeUnit.SECONDS, RetryAssert.sizeOf(manager.getCache(ASYNCHRONOUS_CACHE)), Is.is(2000));
            }
        } finally {
            destroyCluster(cluster);
        }
    }


    @Test
    public void testBootstrap() throws CacheException, RemoteException {
        List<CacheManager> cluster = createCluster(5, ASYNCHRONOUS_CACHE);
        try {
            final Ehcache cache1 = cluster.get(0).getEhcache(ASYNCHRONOUS_CACHE);

            //load up some data
            StopWatch stopWatch = new StopWatch();
            setUpCache(2, cache1);
            long elapsed = stopWatch.getElapsedTime();
            long putTime = ((elapsed / 1000));
            LOG.info("Put Elapsed time: " + putTime);

            assertEquals(2000, cache1.getSize());

            for (CacheManager manager : cluster) {
                RetryAssert.assertBy(7, TimeUnit.SECONDS, RetryAssert.sizeOf(manager.getCache(ASYNCHRONOUS_CACHE)), Is.is(2000));
            }

            //now test bootstrap
            cluster.get(0).addCache("bootStrapResults");
            Cache cache = cluster.get(0).getCache("bootStrapResults");
            List<CachePeer> cachePeers = cluster.get(0).getCacheManagerPeerProvider("RMI").listRemoteCachePeers(cache1);
            CachePeer cachePeer = cachePeers.get(0);

            List<Serializable> keys = cachePeer.getKeys();
            assertEquals(2000, keys.size());

            Element firstElement = cachePeer.getQuiet(keys.get(0));
            long size = firstElement.getSerializedSize();
            assertEquals(517, size);

            int chunkSize = (int) (5000000 / size);

            List<Serializable> requestChunk = new ArrayList<>();
            for (Serializable serializable : keys) {
                requestChunk.add(serializable);
                if (requestChunk.size() == chunkSize) {
                    fetchAndPutElements(cache, requestChunk, cachePeer);
                    requestChunk.clear();
                }
            }
            //get leftovers
            fetchAndPutElements(cache, requestChunk, cachePeer);

            assertEquals(keys.size(), cache.getSize());
        } finally {
            destroyCluster(cluster);
        }
    }

    private void fetchAndPutElements(Ehcache cache, List<Serializable> requestChunk, CachePeer cachePeer) throws RemoteException {
        List<Element> receivedChunk = cachePeer.getElements(requestChunk);
        for (Element element : receivedChunk) {
            assertNotNull(element);
            cache.put(element, true);
        }
    }


    /**
     * Drive everything to point of breakage within a 64MB VM.
     */
    @Test
    public void xTestHugePutsBreaksAsynchronous() throws CacheException {
        List<CacheManager> cluster = createCluster(5, ASYNCHRONOUS_CACHE);
        try {
            final Ehcache cache1 = cluster.get(0).getEhcache(ASYNCHRONOUS_CACHE);

            //Give everything a chance to startup
            StopWatch stopWatch = new StopWatch();
            setUpCache(500, cache1);
            long elapsed = stopWatch.getElapsedTime();
            long putTime = ((elapsed / 1000));
            LOG.info("Put Elapsed time: " + putTime);
            //assertTrue(putTime < 8);

            assertEquals(100000, cache1.getSize());

            for (CacheManager manager : cluster) {
                RetryAssert.assertBy(100, TimeUnit.SECONDS, RetryAssert.sizeOf(manager.getCache(ASYNCHRONOUS_CACHE)), Is.is(20000));
            }
        } finally {
            destroyCluster(cluster);
        }
    }

    void setUpCache(int times, Ehcache cache) {
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < 1000; j++) {
                cache.put(new Element(1000 * i + j, VALUE));
            }
        }
    }


    /**
     * Performance and capacity tests.
     * <p>
     * The numbers given are for the remote peer tester (java -jar ehcache-1.x-remote-debugger.jar ehcache-distributed1.xml)
     * running on a 10Mbit ethernet network and are measured from the time the peer starts receiving to when
     * it has fully received.
     * <p>
     * 4 seconds to get all remove notifications with 6 peers, 5000 Elements and 400 byte payload
     */
    @Test
    public void testBigRemovesProgagatesAsynchronous() throws CacheException, InterruptedException {
        List<CacheManager> cluster = createCluster(5, ASYNCHRONOUS_CACHE);
        try {
            final Ehcache cache = cluster.get(0).getEhcache(ASYNCHRONOUS_CACHE);

            //Give everything a chance to startup
            setUpCache(5, cache);

            Ehcache[] caches = {
                    cache,
                    cluster.get(1).getCache(ASYNCHRONOUS_CACHE),
                    cluster.get(2).getCache(ASYNCHRONOUS_CACHE),
                    cluster.get(3).getCache(ASYNCHRONOUS_CACHE),
                    cluster.get(4).getCache(ASYNCHRONOUS_CACHE)};

            waitForCacheSize(5000, 25, caches);
            //Let the disk stores catch up before the next stage of the test
            sleep(2000);

            IntStream.range(0, 5000).forEach(cache::remove);

            long timeForPropagate = waitForCacheSize(0, 25, caches);
            LOG.info("Remove Elapsed time: " + timeForPropagate);
        } finally {
            destroyCluster(cluster);
        }
    }

    @SuppressWarnings("BusyWait")
    public long waitForCacheSize(long size, int maxSeconds, Ehcache... caches) throws InterruptedException {

        StopWatch stopWatch = new StopWatch();
        while (checkForCacheSize(size, caches)) {
            sleep(500);
            if (stopWatch.getElapsedTime() > maxSeconds * 1000L) {
                fail("Caches still haven't reached the expected size after " + maxSeconds + " seconds");
            }
        }

        return stopWatch.getElapsedTime();
    }

    private boolean checkForCacheSize(long size, Ehcache... caches) {
        return stream(caches).noneMatch(cache -> cache.getSize() != size);
    }


    /**
     * Performance and capacity tests.
     * <p>
     * 5 seconds to send all notifications synchronously with 5 peers, 2000 Elements and 400 byte payload
     * The numbers given below are for the remote peer tester (java -jar ehcache-1.x-remote-debugger.jar ehcache-distributed1.xml)
     * running on a 10Mbit ethernet network and are measured from the time the peer starts receiving to when
     * it has fully received.
     */
    @Test
    public void testBigPutsProgagatesSynchronous() throws CacheException {
        List<CacheManager> cluster = createCluster(5, SYNCHRONOUS_CACHE);
        try {
            //Give everything a chance to startup
            StopWatch stopWatch = new StopWatch();

            setUpCache(2, cluster.get(0).getCache(SYNCHRONOUS_CACHE));

            long elapsed = stopWatch.getElapsedTime();
            long putTime = ((elapsed / 1000));
            LOG.info("Put and Propagate Synchronously Elapsed time: {} seconds", putTime);

            for (CacheManager manager : cluster) {
                assertThat(manager.getCache(SYNCHRONOUS_CACHE).getSize())
                        .describedAs(manager.getName())
                        .isEqualTo(2000);
            }
        } finally {
            destroyCluster(cluster);
        }
    }

    /**
     * Enables long stabilty runs using replication to be done.
     * <p>
     * This test has been run in a profile for 15 hours without any observed issues.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public void manualStabilityTest() {
        List<CacheManager> cluster = createCluster(5, ASYNCHRONOUS_CACHE);
        try {
            AbstractCacheTest.forceVMGrowth();

            ManagementService.registerMBeans(cluster.get(2), AbstractCacheTest.createMBeanServer(), true, true, true, true, true);
            while (true) {
                final Ehcache cache1 = cluster.get(0).getEhcache(ASYNCHRONOUS_CACHE);

                StopWatch stopWatch = new StopWatch();
                setUpCache(2, cache1);

                long elapsed = stopWatch.getElapsedTime();
                long putTime = ((elapsed / 1000));
                LOG.info("Put Elapsed time: " + putTime);
                //assertTrue(putTime < 8);

                for (CacheManager manager : cluster) {
                    RetryAssert.assertBy(2, TimeUnit.SECONDS, RetryAssert.sizeOf(manager.getCache(ASYNCHRONOUS_CACHE)), Is.is(2000));
                }
            }
        } finally {
            destroyCluster(cluster);
        }
    }

    /**
     * Shows result of perf problem and fix in flushReplicationQueue
     * <p>
     * Behaviour before change:
     * <p>
     * INFO: Items written: 10381
     * Oct 29, 2007 11:40:04 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 29712
     * Oct 29, 2007 11:40:57 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 1
     * Oct 29, 2007 11:40:58 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 32354
     * Oct 29, 2007 11:42:34 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 322
     * Oct 29, 2007 11:42:35 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 41909
     * <p>
     * Behaviour after change:
     * INFO: Items written: 26356
     * Oct 29, 2007 11:44:39 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 33656
     * Oct 29, 2007 11:44:40 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 32234
     * Oct 29, 2007 11:44:42 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 38677
     * Oct 29, 2007 11:44:43 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 43418
     * Oct 29, 2007 11:44:44 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 31277
     * Oct 29, 2007 11:44:45 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 27769
     * Oct 29, 2007 11:44:46 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 29596
     * Oct 29, 2007 11:44:47 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 17142
     * Oct 29, 2007 11:44:48 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 14775
     * Oct 29, 2007 11:44:49 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 4088
     * Oct 29, 2007 11:44:51 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 5492
     * Oct 29, 2007 11:44:52 AM net.sf.ehcache.distribution.RMICacheReplicatorTest testReplicatePerf
     * INFO: Items written: 10188
     * <p>
     * Also no pauses noted.
     */
    @Test
    public void testReplicatePerf() {
        List<CacheManager> cluster = createCluster(1, ASYNCHRONOUS_CACHE);
        try {
            Ehcache cache1 = cluster.get(0).getEhcache(ASYNCHRONOUS_CACHE);
            long start = System.nanoTime();
            final String keyBase = Long.toString(start);
            int count = 0;

            Random random = new SecureRandom();
            for (int i = 0; i < 100000; i++) {
                final String key = keyBase + ':' + random.nextInt(1000);
                cache1.put(new Element(key, "My Test"));
                cache1.get(key);
                cache1.remove(key);
                count++;

                final long end = System.nanoTime();
                if (end - start >= TimeUnit.SECONDS.toNanos(1)) {
                    start = end;
                    LOG.info("Items written: " + count);
                    //make sure it does not choke
                    assertTrue("Got only to " + count + " in 1 second!", count > 1000);
                    count = 0;
                }
            }
        } finally {
            destroyCluster(cluster);
        }
    }

    /**
     * Non JUnit invocation of stability test to get cleaner run
     */
    public static void main(String[] args) throws Exception {
        RMICacheReplicatorPerfTest replicatorTest = new RMICacheReplicatorPerfTest();
        replicatorTest.setUp();
        try {
            replicatorTest.manualStabilityTest();
        } finally {
            replicatorTest.noReplicationThreads();
        }
    }
}
