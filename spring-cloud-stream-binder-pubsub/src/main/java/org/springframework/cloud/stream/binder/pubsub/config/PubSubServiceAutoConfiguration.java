/*
 *  Copyright 2016 original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.springframework.cloud.stream.binder.pubsub.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.pubsub.PubSubExtendedBindingProperties;
import org.springframework.cloud.stream.binder.pubsub.PubSubMessageChannelBinder;
import org.springframework.cloud.stream.binder.pubsub.PubSubResourceManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.cloud.pubsub.PubSub;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@ConditionalOnBean(PubSub.class)
@EnableConfigurationProperties({ PubSubBinderConfigurationProperties.class,
		PubSubExtendedBindingProperties.class })
public class PubSubServiceAutoConfiguration {

	@Autowired
	private PubSubExtendedBindingProperties pubSubExtendedBindingProperties;

	@Autowired
	private PubSubBinderConfigurationProperties pubSubBinderConfigurationProperties;

	@Bean
	public PubSubResourceManager pubSubResourceManager(PubSub pubSub) {
		return new PubSubResourceManager(pubSub);
	}

	@Bean
	public PubSubMessageChannelBinder binder(PubSubResourceManager resourceManager)
			throws Exception {
		return new PubSubMessageChannelBinder(pubSubBinderConfigurationProperties,
				pubSubExtendedBindingProperties, resourceManager);
	}

}
