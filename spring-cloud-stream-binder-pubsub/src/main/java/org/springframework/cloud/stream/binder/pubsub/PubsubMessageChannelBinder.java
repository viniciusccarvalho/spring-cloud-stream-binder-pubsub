/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.pubsub;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.pubsub.config.	PubsubBinderConfigurationProperties;
import org.springframework.context.Lifecycle;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.endpoint.SourcePollingChannelAdapter;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.splitter.DefaultMessageSplitter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;


/**
 * @author Vinicius Carvalho
 */
public class PubsubMessageChannelBinder
		extends
		AbstractBinder<MessageChannel, ExtendedConsumerProperties<PubsubConsumerProperties>, ExtendedProducerProperties<PubsubProducerProperties>>
		implements
		ExtendedPropertiesBinder<MessageChannel, PubsubConsumerProperties, PubsubProducerProperties> {

	final String TOPIC_NAME_PATTERN = "projects/%s/topics/%s";

	final String SUBSCRIPTION_NAME_PATTERN = "projects/%s/subscriptions/%s";

	private PubsubExtendedBindingProperties extendedBindingProperties = new PubsubExtendedBindingProperties();

	private PubsubBinderConfigurationProperties configurationProperties;

	public static final String TOPIC_NAME = "TOPIC_NAME";

	public PubsubMessageChannelBinder(PubsubBinderConfigurationProperties configurationProperties) {
		this.configurationProperties = configurationProperties;
		Assert.notNull(configurationProperties.getProjectName(),
				"You must set the google cloud project name to use pub sub");
	}

	@Override
	protected Binding<MessageChannel> doBindConsumer(String name, String group,
			MessageChannel inputTarget,
			ExtendedConsumerProperties<PubsubConsumerProperties> properties) {
		DefaultBinding<MessageChannel> consumerBinding;
		boolean partitioned = properties.isPartitioned();
		Integer partitionIndex = null;
		if (partitioned) {
			partitionIndex = properties.getInstanceIndex();
		}
		Pubsub client = createPubSubClient();
		String topicName = createTopicName(name, properties.getExtension().getPrefix(),
				partitionIndex);
		Topic topic = createTopic(client,topicName);
		Subscription subscription = createSubscription(client,
				createSubscriptionName(name, group, partitionIndex), topic, properties);

		PubSubMessageDrivenChannelAdapter adapter = new PubSubMessageDrivenChannelAdapter(client,subscription,properties);
		adapter.setBeanFactory(getBeanFactory());

		DirectChannel bridgeChannel = new DirectChannel();
		bridgeChannel.setBeanFactory(getBeanFactory());
		bridgeChannel.setBeanName(subscription.getName()+".bridge");

		adapter.setOutputChannel(bridgeChannel);
		adapter.afterPropertiesSet();

		ReceivingHandler convertingBridge = new ReceivingHandler();
		convertingBridge.setOutputChannel(inputTarget);
		convertingBridge.setBeanName(subscription.getName() + ".convert.bridge");
		convertingBridge.afterPropertiesSet();
		bridgeChannel.subscribe(convertingBridge);

		consumerBinding = new DefaultBinding<MessageChannel>(name, group, inputTarget,
				adapter) {
			@Override
			protected void afterUnbind() {

			}
		};

		adapter.start();
		return consumerBinding;
	}

	@Override
	protected Binding<MessageChannel> doBindProducer(String name,
			MessageChannel outboundBindTarget,
			ExtendedProducerProperties<PubsubProducerProperties> properties) {

		boolean partitioned = properties.isPartitioned();
		Pubsub client = createPubSubClient();
		if (partitioned) {
			createTopics(client, name, properties);
		}
		else {
			String topicName = createTopicName(name, properties.getExtension().getPrefix(), null);
			createTopic(client,topicName);
		}
		PubSubMessageHandler delegate = new PubSubMessageHandler(client);

		MessageHandler handler = new SendingHandler(delegate, properties, name);
		EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) outboundBindTarget, handler);
		consumer.setBeanFactory(getBeanFactory());
		consumer.setBeanName("outbound." + name);
		consumer.afterPropertiesSet();
		DefaultBinding<MessageChannel> producerBinding = new DefaultBinding<>(name, null,
				outboundBindTarget, consumer);
		consumer.start();
		return producerBinding;
	}

	private Pubsub createPubSubClient(){
		return getBeanFactory().getBean(Pubsub.class);
	}

	@Override
	public PubsubConsumerProperties getExtendedConsumerProperties(String channelName) {
		return extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public PubsubProducerProperties getExtendedProducerProperties(String channelName) {
		return extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	public void setExtendedBindingProperties(
			PubsubExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	public PubsubBinderConfigurationProperties getConfigurationProperties() {
		return configurationProperties;
	}

	private void createTopics(Pubsub client, String name, ExtendedProducerProperties<PubsubProducerProperties> properties) {
		for (int i = 0; i < properties.getPartitionCount(); i++) {
			String topicName = createTopicName(name, properties.getExtension().getPrefix(), i);
			createTopic(client,topicName);
		}
	}

	private Topic createTopic(Pubsub client, String name) {
		String topicName = String.format(TOPIC_NAME_PATTERN,
				configurationProperties.getProjectName(), name);
		Topic topic = null;
		try {
			logger.debug(String.format("Creating topic: %s",topicName));
			topic = client.projects().topics().create(topicName, new Topic()).execute();
		}
		catch (IOException e) {
			if (e instanceof GoogleJsonResponseException) {
				GoogleJsonResponseException je = (GoogleJsonResponseException) e;
				if (je.getStatusCode() == 409) {
					logger.warn(String.format("Topic %s already exists",topicName));
					topic = new Topic();
					topic.setName(topicName);
				}
			}
		}
		return topic;
	}

	private String createTopicName(String name, String prefix, Integer partitionIndex) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(applyPrefix(prefix, name));

		if (partitionIndex != null) {
			buffer.append("-" + partitionIndex);
		}
		return buffer.toString();
	}

	private Subscription createSubscription(Pubsub client, String name, Topic topic,
			ExtendedConsumerProperties<PubsubConsumerProperties> properties) {
		String subscriptionName = String.format(SUBSCRIPTION_NAME_PATTERN,
				configurationProperties.getProjectName(), name);
		Subscription subscription = null;
		try {
			Subscription configuration = new Subscription();
			configuration.setTopic(topic.getName());
			configuration.setAckDeadlineSeconds(properties.getExtension()
					.getAckDeadlineSeconds());
			logger.debug(String.format("Creating subscription: %s linking to topic %s ",subscriptionName, topic.getName()));
			subscription = client.projects().subscriptions()
					.create(subscriptionName, configuration).execute();
		}
		catch (IOException e) {
			if (e instanceof GoogleJsonResponseException) {
				GoogleJsonResponseException je = (GoogleJsonResponseException) e;
				if (je.getStatusCode() == 409) {
					logger.warn(String.format("Subscription %s already exists",subscriptionName));
					subscription = new Subscription();
					subscription.setName(subscriptionName);
					subscription.setTopic(topic.getName());
					subscription.setAckDeadlineSeconds(properties.getExtension()
							.getAckDeadlineSeconds());
				}
			}
		}
		return subscription;
	}

	private String createSubscriptionName(String name, String group,
			Integer partitionIndex) {
		boolean anonymousConsumer = !StringUtils.hasText(group);
		StringBuffer buffer = new StringBuffer();
		if (anonymousConsumer) {
			buffer.append(groupedName(name, UUID.randomUUID().toString()));
		}
		else {
			buffer.append(groupedName(name, group));
		}

		if (partitionIndex != null) {
			buffer.append("-" + partitionIndex);
		}
		return buffer.toString();
	}

	private final class SendingHandler extends AbstractMessageHandler implements Lifecycle {

		private final MessageHandler delegate;
		private final PartitionHandler partitionHandler;
		private ExtendedProducerProperties<PubsubProducerProperties> properties;
		private final String name;

		private SendingHandler(MessageHandler delegate, ExtendedProducerProperties<PubsubProducerProperties> properties, String name) {
			this.delegate = delegate;
			this.name = name;
			this.properties = properties;
			this.partitionHandler = new PartitionHandler(PubsubMessageChannelBinder.this.getBeanFactory(), evaluationContext, partitionSelector,
					properties);
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			MessageValues messageToSend = serializePayloadIfNecessary(message);
			Integer partitionIndex = null;
			if (properties.isPartitioned()) {
				partitionIndex = partitionHandler.determinePartition(message);
			}
			String topicName = createTopicName(name, properties.getExtension().getPrefix(), partitionIndex);
			logger.debug(String.format("Sending message to topic %s at partition index: %d",topicName,partitionIndex));
			messageToSend.put(TOPIC_NAME, String.format(TOPIC_NAME_PATTERN,
					configurationProperties.getProjectName(), topicName));
			this.delegate.handleMessage(messageToSend.toMessage(getMessageBuilderFactory()));
		}

		@Override
		public void start() {
			if (this.delegate instanceof Lifecycle) {
				((Lifecycle) this.delegate).start();
			}
		}

		@Override
		public void stop() {
			if (this.delegate instanceof Lifecycle) {
				((Lifecycle) this.delegate).stop();
			}
		}

		@Override
		public boolean isRunning() {
			if (this.delegate instanceof Lifecycle) {
				return ((Lifecycle) this.delegate).isRunning();
			}
			else {
				return true;
			}
		}
	}

	private final class ReceivingHandler extends AbstractReplyProducingMessageHandler {
		private Logger logger = LoggerFactory.getLogger(ReceivingHandler.class);

		private ReceivingHandler() {
			super();
			this.setBeanFactory(PubsubMessageChannelBinder.this.getBeanFactory());
		}

		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			PubsubMessage message = (PubsubMessage) requestMessage.getPayload();
			logger.debug("Received message id: {}",message.getMessageId());
			return deserializePayloadIfNecessary(
					getMessageBuilderFactory().withPayload(message.decodeData())
							.copyHeaders(requestMessage.getHeaders()).build()).toMessage(
					getMessageBuilderFactory());
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			/*
			 * we've already copied the headers so no need for the ARPMH to do it, and we
			 * don't want the content-type restored if absent.
			 */
			return false;
		}

	}

}
