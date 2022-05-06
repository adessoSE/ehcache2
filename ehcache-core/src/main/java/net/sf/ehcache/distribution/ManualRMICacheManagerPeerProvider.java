/**
 * Copyright Terracotta, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <a href="http://www.apache.org/licenses/LICENSE-2.0">http://www.apache.org/licenses/LICENSE-2.0</a>
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package net.sf.ehcache.distribution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * A provider of Peer RMI addresses based off manual configuration.
 * <p>
 * Because there is no monitoring of whether a peer is actually there, the list of peers is dynamically
 * looked up and verified each time a lookup request is made.
 * <p>
 *
 * @author Greg Luck
 * @version $Id$
 */
public class ManualRMICacheManagerPeerProvider extends RMICacheManagerPeerProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ManualRMICacheManagerPeerProvider.class.getName());

    /**
     * Contains a RMI URLs of the form: "//" + hostName + ":" + port + "/" + cacheName;
     */
    protected final Set<String> peerUrls = Collections.synchronizedSet(new HashSet<>());

    /**
     * Empty constructor.
     */
    public ManualRMICacheManagerPeerProvider() {
        super();
    }

    @Override
    synchronized Set<String> getRegisteredRmiUrls() {
        return new TreeSet<>(peerUrls);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init() {
        //nothing to do here
    }

    /**
     * Time for a cluster to form. This varies considerably, depending on the implementation.
     *
     * @return the time in ms, for a cluster to form
     */
    @Override
    public long getTimeForClusterToForm() {
        return 0;
    }

    @Override
    public synchronized void registerPeer(String rmiUrl) {
        peerUrls.add(rmiUrl);
    }

    @Override
    public synchronized void unregisterPeer(String rmiUrl) {
        peerUrls.remove(rmiUrl);
    }
}
