package net.sf.ehcache.distribution;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.distribution.MulticastRMICacheManagerPeerProvider.CachePeerState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import static net.sf.ehcache.distribution.MulticastRMICacheManagerPeerProvider.PeerState.STALE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class MulticastRMICacheManagerPeerProviderTest {

    @Mock
    private CacheManager cacheManager = mock(CacheManager.class);

    @Mock
    private InetAddress groupMulticastAdress;

    @Mock
    private InetAddress hostAddress;

    private final int groupMulticastPort = 40001;

    private final int timeToLive = 1;

    MulticastRMICacheManagerPeerProvider sut;

    @Before
    public void setUp() throws Exception {
        sut = new MulticastRMICacheManagerPeerProvider(cacheManager, groupMulticastAdress, groupMulticastPort, timeToLive, hostAddress);
    }

    @Test
    public void checkActivePeerEntries() {
        Map<String, CachePeerState> urls = new HashMap<>();
        CachePeerState cachePeerState = mock(CachePeerState.class);
        given(cachePeerState.peerState()).willReturn(STALE);
        urls.put("//localhost/X:40002", cachePeerState);
        urls.put("//localhost/Y:40002", Mockito.mock(CachePeerState.class));

        sut.checkActivePeerEntries(urls.entrySet());

        assertThat(urls).hasSize(1);
    }
}