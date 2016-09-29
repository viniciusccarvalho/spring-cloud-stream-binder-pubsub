package org.springframework.cloud.stream.binder.pubsub;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.cloud.AuthCredentials;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.ReceivedMessage;
import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.cloud.pubsub.TopicInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Vinicius Carvalho
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = PubSubMessageHandlerTests.PubSubMessageHandlerApplicationTest.class)

public class PubSubMessageHandlerTests {

	@Autowired
	private PubSubResourceManager resourceManager;
	private Logger logger = LoggerFactory.getLogger(PubSubMessageHandlerTests.class);

	@Autowired
	private PubSub pubSub;


	@Test
	public void loadTests() throws Exception{
		SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
		int partitionCount = 5;
		int messageCount = 100000;
		List<TopicInfo> topics = new ArrayList<>();
		String baseTopicName = "pubsub-test";
		ExtendedProducerProperties<PubSubProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(new PubSubProducerProperties());
		extendedProducerProperties.setPartitionCount(partitionCount);
		extendedProducerProperties.setPartitionKeyExpression(spelExpressionParser.parseExpression("payload"));


		for(int i=0;i<partitionCount;i++){
			topics.add(resourceManager.declareTopic(baseTopicName,"",i));
		}
		PubSubMessageHandler messageHandler = new PubSubMessageHandler(resourceManager,extendedProducerProperties,topics);
		messageHandler.start();
		Long start = System.currentTimeMillis();
		for(int j=0;j<messageCount;j++){
			String payload = "foo-"+j;
			messageHandler.handleMessage(MessageBuilder.withPayload(payload.getBytes()).setHeader(BinderHeaders.PARTITION_HEADER,j%partitionCount).build());
		}
		System.out.println("Total time: " + (System.currentTimeMillis()-start));
		Thread.sleep(30000L);

	}

	@Test
	public void pullSync() throws Exception {
		Subscription subscription = pubSub.getSubscription("test");

		CountDownLatch latch = new CountDownLatch(1);
		subscription.pullAsync(message -> {
			System.out.println(message.payloadAsString());
			latch.countDown();
		});

		latch.await();
	}

	@Test
	public void consumeMessages() throws Exception {

		int messageCount = 2000;
		CountDownLatch latch = new CountDownLatch(messageCount);
		String baseTopicName = "pubsub-test";
		ExtendedProducerProperties<PubSubProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(new PubSubProducerProperties());
		List<TopicInfo> topics = new ArrayList<>();
		topics.add(resourceManager.declareTopic(baseTopicName,"",null));
		SubscriptionInfo subscriptionInfo = resourceManager.declareSubscription(topics.get(0).name(),"test-subscription","");
		PubSubMessageHandler messageHandler = new PubSubMessageHandler(resourceManager,extendedProducerProperties,topics);
		messageHandler.start();
		resourceManager.createConsumer(subscriptionInfo, message -> {
			latch.countDown();
			logger.info("Received message : {}", message.payloadAsString());
		});
		for(int j=0;j<messageCount;j++){
			String payload = "foo-"+j;
			messageHandler.handleMessage(MessageBuilder.withPayload(payload.getBytes()).build());
		}
		latch.await();
	}


	@SpringBootApplication
	@EnableConfigurationProperties({PubSubBinderConfigurationProperties.class, PubSubExtendedBindingProperties.class})
	static class PubSubMessageHandlerApplicationTest {
		@Autowired
		private PubSubExtendedBindingProperties pubSubExtendedBindingProperties;

		@Bean
		public PubSub pubSub(@Value("${google.cloud.json.cred}") String creds) throws Exception {

			return PubSubOptions.builder().authCredentials(AuthCredentials.createForJson(new ByteArrayInputStream(creds.getBytes()))).build().service();
		}

		@Bean
		public PubSubResourceManager pubSubResourceManager(PubSub pubSub){
			return new PubSubResourceManager(pubSub);
		}


	}

}
