package net.sf.ehcache.distribution;

import net.sf.ehcache.Ehcache;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.test.categories.CheckShorts;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@Category(CheckShorts.class)
@RunWith(MockitoJUnitRunner.class)
public class RMIAsynchronousCacheReplicatorTest {

    RMIAsynchronousCacheReplicator sut = new RMIAsynchronousCacheReplicator(
            true,
            true,
            true,
            true,
            true,
            1000,
            1000);

    @Mock
    Ehcache cache;

    @Test
    public void extractEventMessagesWithoutDroppedMessages() {
        // given
        List<RmiEventMessage> sourceMessages = IntStream.range(1, 21)
                .mapToObj((key) -> new RmiEventMessage(cache, key))
                .collect(Collectors.toList());

        // when
        LinkedList<Object> queue = new LinkedList<>(sourceMessages);
        List<RmiEventMessage> rmiEventMessages = sut.extractEventMessages(queue, 10);

        // then
        RmiEventMessage[] messages = sourceMessages.subList(0, 10).toArray(new RmiEventMessage[]{});
        assertThat(rmiEventMessages).hasSize(10).containsExactly(messages);
        assertThat(queue).hasSize(10);
    }

    @Test
    public void extractEventMessagesWithSoftReferenceAndWithoutDroppedMessages() {
        // given
        List<Object> sourceMessages = IntStream.range(1, 21)
                .mapToObj((key) -> new RmiEventMessage(cache, key))
                .collect(Collectors.toList());
        SoftReference<RmiEventMessage> reference = mock(SoftReference.class);
        RmiEventMessage firstElement = (RmiEventMessage) sourceMessages.get(0);
        given(reference.get()).willReturn(firstElement);
        sourceMessages.set(0, reference);

        // when
        LinkedList<Object> queue = new LinkedList<>(sourceMessages);
        List<RmiEventMessage> rmiEventMessages = sut.extractEventMessages(queue, 10);

        // then
        List<Object> queueWithoutReference = new ArrayList<>(sourceMessages.subList(1, 10));
        queueWithoutReference.add(0, firstElement);
        RmiEventMessage[] messages = queueWithoutReference.toArray(new RmiEventMessage[]{});
        assertThat(rmiEventMessages).hasSize(10).containsExactly(messages);
        assertThat(queue).hasSize(10);
    }

    @Test
    public void extractEventMessagesWithDroppedMessages() {
        SoftReference<RmiEventMessage> reference = mock(SoftReference.class);
        given(reference.get()).willReturn(null);

        // given
        List<Object> sourceMessages = IntStream.range(1, 21)
                .mapToObj((key) -> {
                    if (key == 1) {
                        return reference;
                    }
                    return new RmiEventMessage(cache, key);
                })
                .collect(Collectors.toList());

        // when
        LinkedList<Object> queue = new LinkedList<>(sourceMessages);
        List<RmiEventMessage> rmiEventMessages = sut.extractEventMessages(queue, 10);

        // then
        RmiEventMessage[] messages = sourceMessages.subList(1, 11).toArray(new RmiEventMessage[]{});
        assertThat(rmiEventMessages).hasSize(10).containsExactly(messages);
        assertThat(queue).hasSize(9);

    }

    @Test
    public void extractEventMessagesWithLessEntriesInQueueThanMaxListSize() {
        // given
        List<RmiEventMessage> queue = IntStream.range(1, 21)
                .mapToObj((key) -> new RmiEventMessage(cache, key))
                .collect(Collectors.toList());

        // when
        List<RmiEventMessage> rmiEventMessages = sut.extractEventMessages(new LinkedList<>(queue), 100);

        // then
        RmiEventMessage[] messages = queue.toArray(new RmiEventMessage[]{});
        assertThat(rmiEventMessages).hasSize(20).containsExactly(messages);
    }
}