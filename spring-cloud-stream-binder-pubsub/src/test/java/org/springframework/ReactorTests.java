package org.springframework;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.google.api.SystemParameter;
import com.google.cloud.pubsub.TopicInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;

/**
 * @author Vinicius Carvalho
 */
public class ReactorTests {

	Logger logger = LoggerFactory.getLogger(ReactorTests.class);

	@Test
	public void testGroupTopics() throws Exception {
		Flux<Message> flux = createFlux();
//		flux.window(10,Duration.ofMillis(100)).flatMap(w ->
//				w.groupBy(Message::getTopic)
//						.flatMap(group -> group.reduce(0, (count, message) -> count++)
//								.map(total -> {return Collections.singletonMap(group.key(),total);}))).subscribe(System.out::println);


		flux.groupBy(Message::getTopic)
		.flatMap(group -> group
				.buffer(1000,Duration.ofMillis(100))

				.map(messages -> { return new GroupedMessage(group.key(),messages); }) )

				.publishOn(Schedulers.elastic())

				.subscribe(entries -> {
					logger.info(entries.toString());
					try {
						Thread.sleep(101);
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				});

		Thread.sleep(2000L);
	}


	@Test
	public void paralleSubscription() throws Exception {
		CountDownLatch latch = new CountDownLatch(5);
		Flux.fromArray(new Integer[] {1,2,3,4,5}).parallel(8).runOn(Schedulers.elastic()).subscribe(integer -> {
			logger.info(""+integer);
			try {
				Thread.sleep(1000);
				latch.countDown();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}

		});

		latch.await();
	}

	@Test
	public void workQueueConsumerTest() throws Exception {
		WorkQueueProcessor processor = WorkQueueProcessor.create();
		processor.log().subscribe(o -> {
			System.out.println(o);
		});
		processor.onNext("foo");
		Thread.sleep(1000L);

	}

	@Test
	public void multipleSubscribers() throws Exception {
		Flux<Long> flux = Flux.intervalMillis(100);
		flux.subscribe(System.out::println);
		flux.subscribe(System.err::println);
		Thread.sleep(1000);
	}

	@Test
	public void multipleFilteredSubscribers() throws Exception {
		Flux<Long> flux = Flux.intervalMillis(100);

		flux.filter(aLong -> aLong%2==0).publishOn(Schedulers.elastic()).subscribe(aLong -> logger.info("Even printer: " + aLong));
		flux.filter(aLong -> aLong%2==1).publishOn(Schedulers.elastic()).subscribe(aLong -> logger.info("Odd printer: " + aLong));
		Thread.sleep(1000);
	}



	private Flux<Message> createFlux() {


		final int count = 1000000;
		return Flux.create(emitter -> {
			new Thread(() -> {
				Random random = new Random();
				for(int i=0;i<count;i++){
					int r = random.nextInt(100);
					int j = (r>10) ? 1 : 2;
					emitter.next(new Message(TopicInfo.of("Topic-"+j),"foo".getBytes()));

				}
				emitter.complete();
			}).run();
		});

	}

	class GroupedMessage {
		TopicInfo topicInfo;
		List<Message> messages;

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("GroupedMessage{");
			sb.append("topicInfo=").append(topicInfo);
			sb.append(", messages=").append(messages.size());
			sb.append('}');
			return sb.toString();
		}

		public GroupedMessage(TopicInfo topicInfo, List<Message> messages) {
			this.topicInfo = topicInfo;
			this.messages = messages;
		}

		public TopicInfo getTopicInfo() {
			return topicInfo;
		}

		public void setTopicInfo(TopicInfo topicInfo) {
			this.topicInfo = topicInfo;
		}

		public List<Message> getMessages() {
			return messages;
		}

		public void setMessages(List<Message> messages) {
			this.messages = messages;
		}
	}

	class Message {
		public Message(TopicInfo topic, byte[] payload) {
			this.topic = topic;
			this.payload = payload;
		}



		private TopicInfo topic;
		private byte[] payload;

		public TopicInfo getTopic() {
			return topic;
		}

		public void setTopic(TopicInfo topic) {
			this.topic = topic;
		}

		public byte[] getPayload() {
			return payload;
		}

		public void setPayload(byte[] payload) {
			this.payload = payload;
		}

		@Override
		public String toString() {
			final StringBuffer sb = new StringBuffer("Message{");
			sb.append("topic=").append(topic);
			sb.append('}');
			return sb.toString();
		}
	}
}
