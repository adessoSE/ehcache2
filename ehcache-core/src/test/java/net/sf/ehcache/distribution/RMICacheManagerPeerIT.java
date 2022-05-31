/**
 *  Copyright Terracotta, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      <a href="http://www.apache.org/licenses/LICENSE-2.0">http://www.apache.org/licenses/LICENSE-2.0</a>
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.sf.ehcache.distribution;


import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.util.RetryAssert;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for RMICachePeer
 * <p>
 * Note these tests need a live network interface running in multicast mode to work
 *
 * @author <a href="mailto:gluck@thoughtworks.com">Greg Luck</a>
 * @version $Id$
 */
public class RMICacheManagerPeerIT extends AbstractRMITest {

    private static final Logger LOG = LoggerFactory.getLogger(RMICacheManagerPeerIT.class.getName());

    @After
    public void tearDown() throws InterruptedException {
        RetryAssert.assertBy(30, TimeUnit.SECONDS, AbstractRMITest::getActiveReplicationThreads, IsEmptyCollection.empty());
    }


    /**
     * Can we create the peer using remote port of 0?
     */
    @Test
    public void testCreatePeerWithAutomaticRemotePort() throws RemoteException {
        Cache cache = new Cache(new CacheConfiguration().name("test").maxEntriesLocalHeap(10));
        for (int i = 0; i < 10; i++) {
            new RMICachePeer(cache, "localhost", 5010, 0, 2000);
        }
    }


    /**
     * Can we create the peer using a specified free remote port of 45000
     */
    @Test
    public void testCreatePeerWithSpecificRemotePort() throws RemoteException {
        Cache cache = new Cache(new CacheConfiguration().name("test").maxEntriesLocalHeap(10));
        for (int i = 0; i < 10; i++) {
            new RMICachePeer(cache, "localhost", 5010, 45000, 2000);
        }
    }


    /**
     * See if socket.setSoTimeout(socketTimeoutMillis) works. Should throw a SocketTimeoutException
     */
    @Test
    public void testFailsIfTimeoutExceeded() throws Exception {
        CacheManager manager = new CacheManager(new Configuration().name("testFailsIfTimeoutExceeded"));
        try {
            Cache cache = new Cache(new CacheConfiguration().name("test").maxEntriesLocalHeap(10));
            RMICacheManagerPeerListener peerListener = new RMICacheManagerPeerListener("localhost", 5010, 0, manager, 2000);
            try {
                RMICachePeer rmiCachePeer = new SlowRMICachePeer(cache, 1000, 2000);
                peerListener.addCachePeer(cache.getName(), rmiCachePeer);
                peerListener.init();


                try {
                    CachePeer cachePeer = new ManualRMICacheManagerPeerProvider().lookupRemoteCachePeer(rmiCachePeer.getUrl());
                    cachePeer.put(new Element("1", new Date()));
                    fail();
                } catch (UnmarshalException e) {
                    assertEquals(SocketTimeoutException.class, e.getCause().getClass());
                }
            } finally {
                peerListener.dispose();
            }
        } finally {
            manager.shutdown();
        }
    }

    /**
     * See if socket.setSoTimeout(socketTimeoutMillis) works.
     * Should not fail because the put takes less than the timeout.
     */
    @Test
    public void testWorksIfTimeoutNotExceeded() throws Exception {
        CacheManager manager = new CacheManager(new Configuration().name("testWorksIfTimeoutNotExceeded"));
        try {
            Cache cache = new Cache(new CacheConfiguration().name("test").maxEntriesLocalHeap(10));
            RMICacheManagerPeerListener peerListener = new RMICacheManagerPeerListener("localhost", 5010, 0, manager, 2000);
            try {
                RMICachePeer rmiCachePeer = new SlowRMICachePeer(cache, 2000, 0);

                peerListener.addCachePeer(cache.getName(), rmiCachePeer);
                peerListener.init();

                CachePeer cachePeer = new ManualRMICacheManagerPeerProvider().lookupRemoteCachePeer(rmiCachePeer.getUrl());
                cachePeer.put(new Element("1", new Date()));
            } finally {
                peerListener.dispose();
            }
        } finally {
            manager.shutdown();
        }
    }

    /**
     * Test send.
     * <p>
     * This is a unit test because it was throwing AbstractMethodError if a method has changed signature,
     * or NoSuchMethodError is a new one is added. The problem is that rmic needs
     * to recompile the stub after any changes are made to the CachePeer source, something done by ant
     * compile but not by the IDE.
     */
    @Test
    public void testSend() throws Exception {
        CacheManager manager = new CacheManager(new Configuration().name("send"));
        try {
            Cache cache = new Cache(new CacheConfiguration().name("test").maxEntriesLocalHeap(10));
            RMICachePeer rmiCachePeer = new RMICachePeer(cache, "localhost", 5010, 0, 2100);
            RMICacheManagerPeerListener peerListener = new RMICacheManagerPeerListener("localhost", 5010, 0, manager, 2000);
            manager.addCache(cache);

            peerListener.addCachePeer(cache.getName(), rmiCachePeer);
            peerListener.init();

            CachePeer cachePeer = new ManualRMICacheManagerPeerProvider().lookupRemoteCachePeer(rmiCachePeer.getUrl());
            cachePeer.send(Collections.singletonList(new RmiEventMessage(cache, new Element("1", new Date()))));
        } finally {
            manager.shutdown();
        }
    }


    static class SlowRMICachePeer extends RMICachePeer {

        private final long sleepTime;
        
        public SlowRMICachePeer(Ehcache cache, int socketTimeoutMillis, int sleepTime)
                throws RemoteException {
            super(cache, "localhost", 5010, 0, socketTimeoutMillis);
            this.sleepTime = sleepTime;
        }

        @Override
        public void put(Element element) throws RemoteException, IllegalArgumentException, IllegalStateException {
            try {
                sleep(sleepTime);
                super.put(element);
            } catch (InterruptedException e) {
                LOG.error("sleep of put interrupted", e);
            }
        }
    }
}
