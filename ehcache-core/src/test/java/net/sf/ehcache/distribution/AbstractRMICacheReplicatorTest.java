package net.sf.ehcache.distribution;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.test.categories.CheckShorts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@Category(CheckShorts.class)
@RunWith(MockitoJUnitRunner.class)
public class AbstractRMICacheReplicatorTest {

    @Mock
    Ehcache cache;

    AbstractRMICacheReplicator sut = new AbstractRMICacheReplicator(true, true, true, true, true) {
        @Override
        protected void processEvent(RmiEventMessage eventMessage) {

        }
    };

    @Test
    public void processEvent0WithUnserializable() {
        // given
        RmiEventMessage eventMessage = Mockito.mock(RmiEventMessage.class);
        given(eventMessage.isSerializable()).willReturn(false);
        AbstractRMICacheReplicator spy = spy(sut);

        // when
        spy.processEvent0(eventMessage);

        // then
        verify(spy, never()).processEvent(eventMessage);
    }

    @Test
    public void processEvent0WithSerializable() {
        // given
        RmiEventMessage eventMessage = Mockito.mock(RmiEventMessage.class);
        given(eventMessage.isSerializable()).willReturn(true);
        AbstractRMICacheReplicator spy = spy(sut);

        // when
        spy.processEvent0(eventMessage);

        // then
        verify(spy).processEvent(eventMessage);
    }

    @Test
    public void notifyElementRemoved() {

        AbstractRMICacheReplicator spy = spy(sut);
        spy.notifyElementRemoved(cache, new Element("k", "v"));

        verify(spy).processEvent(any());
    }

    @Test
    public void notifyElementEvicted() {

        AbstractRMICacheReplicator spy = spy(sut);
        spy.notifyElementEvicted(null, null);

        verify(spy, never()).processEvent0(any());
    }

    @Test
    public void notifyElementExpired() {
        AbstractRMICacheReplicator spy = spy(sut);
        spy.notifyElementExpired(null, null);

        verify(spy, never()).processEvent0(any());

    }

    @Test
    public void notifyElementUpdated() {
        AbstractRMICacheReplicator spy = spy(sut);
        spy.notifyElementUpdated(cache, new Element("k", "v"));

        verify(spy).processEvent0(any());
    }

    @Test
    public void notifyElementPut() {
        AbstractRMICacheReplicator spy = spy(sut);
        spy.notifyElementPut(cache, new Element("k", "v"));

        verify(spy).processEvent0(any());
    }

    @Test
    public void notifyRemoveAll() {
        AbstractRMICacheReplicator spy = spy(sut);
        spy.notifyRemoveAll(cache);

        verify(spy).processEvent0(any());
    }

    @Test
    public void dispose() {
        // when
        sut.dispose();

        // then
        assertThat(sut.alive()).isFalse();
    }
}