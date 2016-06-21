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

package org.springframework.cloud.stream.binder.pubsub.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.api.services.pubsub.Pubsub;

import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.PubsubConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.PubsubMessageChannelBinder;
import org.springframework.cloud.stream.binder.pubsub.PubsubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubsubBinderConfigurationProperties;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * @author Vinicius Carvalho
 */
public class PubsubTestBinder extends AbstractTestBinder<PubsubMessageChannelBinder, ExtendedConsumerProperties<PubsubConsumerProperties>, ExtendedProducerProperties<PubsubProducerProperties>> {

	private PubsubBinderConfigurationProperties configurationProperties;

	private final Set<String> topics = new HashSet<String>();
	private final Set<String> subscriptions = new HashSet<String>();
	private Pubsub client;

	public PubsubTestBinder(Pubsub pubsub, PubsubBinderConfigurationProperties configurationProperties) {
		this.configurationProperties = configurationProperties;
		GenericApplicationContext context = new GenericApplicationContext();
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(1);
		scheduler.afterPropertiesSet();
		context.getBeanFactory().registerSingleton(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME, scheduler);
		context.refresh();
		PubsubMessageChannelBinder binder = new PubsubMessageChannelBinder(configurationProperties, pubsub);
		binder.setApplicationContext(context);
		setBinder(binder);
		this.client = pubsub;
		this.setBinder(binder);
	}

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel moduleInputChannel, ExtendedConsumerProperties<PubsubConsumerProperties> properties) {
		this.topics.add(name);
		this.subscriptions.add(name);
		return super.bindConsumer(name, group, moduleInputChannel, properties);
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel, ExtendedProducerProperties<PubsubProducerProperties> properties) {
		this.topics.add(name);
		return super.bindProducer(name, moduleOutputChannel, properties);
	}

	@Override
	public void cleanup() {
		for(String topic: topics){
			try {
				client.projects().topics().delete("projects/test/topics/"+topic).execute();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


}
