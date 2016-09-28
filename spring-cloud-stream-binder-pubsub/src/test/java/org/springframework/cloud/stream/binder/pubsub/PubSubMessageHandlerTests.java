package org.springframework.cloud.stream.binder.pubsub;

import java.io.ByteArrayInputStream;

import com.google.cloud.AuthCredentials;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.TopicInfo;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.EvaluationContext;
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

	@Autowired
	private PubSubMessageHandler messageHandler;

	@Test
	public void contextLoads() throws Exception{
		TopicInfo topicInfo = resourceManager.declareTopic("test","",null);
		Message message = MessageBuilder.withPayload("foo".getBytes()).build();
		messageHandler.handleMessage(message);
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

		@Bean
		public PubSubMessageHandler messageHandler(PubSubResourceManager resourceManager, ConfigurableListableBeanFactory beanFactory, EvaluationContext evaluationContext){
			PubSubProducerProperties producerProperties = new PubSubProducerProperties();
			ExtendedProducerProperties<PubSubProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(producerProperties);
			return  new PubSubMessageHandler(resourceManager,"test",extendedProducerProperties, new PartitionHandler(beanFactory,evaluationContext,null,extendedProducerProperties));
		}
	}

}
