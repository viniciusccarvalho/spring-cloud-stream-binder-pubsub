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

package org.springframework.cloud.stream.binder.pubsub;

/**
 * @author Vinicius Carvalho
 */
public class PubSubProducerProperties {
	private String prefix = "";
	private Integer concurrency = null;
	private Integer batchSize = 1000;
	private Integer windowSize = 100;
	private boolean batchEnabled = true;

	public boolean isBatchEnabled() {
		return batchEnabled;
	}

	public void setBatchEnabled(boolean batchEnabled) {
		this.batchEnabled = batchEnabled;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		if(batchSize != null && batchSize > 1)
			this.batchSize = Math.min(1000,batchSize);
	}

	public Integer getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(Integer windowSize) {
		if(windowSize != null && windowSize > 1)
			this.windowSize = windowSize;
	}

	public Integer getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
}
