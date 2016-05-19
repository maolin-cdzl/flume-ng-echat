package com.echat.flume.sink;

import com.google.common.base.Throwables;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;


public class KafkaSink extends AbstractSink implements Configurable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

	private Properties kafkaProps;
	private Producer<String, byte[]> producer;
	private String topic;
	private int batchSize;
	private String keyHeader;
	private boolean prependKey;
	private List<KeyedMessage<String, byte[]>> messageList;

	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = null;
		Event event = null;
		String eventTopic = null;
		String eventKey = null;

		try {
			long processedEvents = 0;

			transaction = channel.getTransaction();
			transaction.begin();

			messageList.clear();
			for (; processedEvents < batchSize; processedEvents += 1) {
				event = channel.take();

				if (event == null) {
					// no events available in channel
					break;
				}

				Map<String, String> headers = event.getHeaders();

				if ((eventTopic = headers.get(KafkaSinkConstants.TOPIC)) == null) {
					eventTopic = topic;
				}

				eventKey = headers.get(keyHeader);
				byte[] eventBody = null;
				if( eventKey != null && prependKey ) {
					byte[] key = eventKey.getBytes();
					byte[] body = event.getBody();
					eventBody = new byte[key.length + body.length + 1];
					System.arraycopy(key,0,eventBody,0,key.length);
					eventBody[key.length] = 32; // space
					System.arraycopy(body,0,eventBody,key.length + 1,body.length);
				} else {
					eventBody = event.getBody();
				}

				if (logger.isDebugEnabled()) {
					logger.debug("{Event} " + eventTopic + " : " + eventKey + " : "
							+ new String(eventBody, "UTF-8"));
					logger.debug("event #{}", processedEvents);
				}

				// create a message and add to buffer
				KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>
					(eventTopic, eventKey, eventBody);
				messageList.add(data);

			}

			// publish batch and commit.
			if (processedEvents > 0) {
				producer.send(messageList);
			}

			transaction.commit();

		} catch (Exception ex) {
			String errorMsg = "Failed to publish events";
			logger.error("Failed to publish events", ex);
			result = Status.BACKOFF;
			if (transaction != null) {
				try {
					transaction.rollback();
				} catch (Exception e) {
					logger.error("Transaction rollback failed", e);
					throw Throwables.propagate(e);
				}
			}
			throw new EventDeliveryException(errorMsg, ex);
		} finally {
			if (transaction != null) {
				transaction.close();
			}
		}

		return result;
	}

	@Override
	public synchronized void start() {
		// instantiate the producer
		ProducerConfig config = new ProducerConfig(kafkaProps);
		producer = new Producer<String, byte[]>(config);
		super.start();
	}

	@Override
	public synchronized void stop() {
		producer.close();
		logger.info("Kafka Sink {} stopped.", getName());
		super.stop();
	}


	/**
	 * We configure the sink and generate properties for the Kafka Producer
	 *
	 * Kafka producer properties is generated as follows:
	 * 1. We generate a properties object with some static defaults that
	 * can be overridden by Sink configuration
	 * 2. We add the configuration users added for Kafka (parameters starting
	 * with .kafka. and must be valid Kafka Producer properties
	 * 3. We add the sink's documented parameters which can override other
	 * properties
	 *
	 * @param context
	 */
	@Override
	public void configure(Context context) {
		keyHeader = context.getString(KafkaSinkConstants.KEY_HEADER,KafkaSinkConstants.DEFAULT_KEY_HEADER);
		logger.info("Using header: " + keyHeader + " as key field");

		prependKey = context.getBoolean(KafkaSinkConstants.PREPEND_KEY,KafkaSinkConstants.DEFAULT_PREPEND_KEY);
		if( prependKey ) {
			logger.info("Will prepend key in message body");
		}

		batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE,
				KafkaSinkConstants.DEFAULT_BATCH_SIZE);
		messageList =
			new ArrayList<KeyedMessage<String, byte[]>>(batchSize);
		logger.debug("Using batch size: {}", batchSize);

		topic = context.getString(KafkaSinkConstants.TOPIC,KafkaSinkConstants.DEFAULT_TOPIC);
		if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
			logger.warn("The Property 'topic' is not set. " +
					"Using the default topic name: " +
					KafkaSinkConstants.DEFAULT_TOPIC);
		} else {
			logger.info("Using the static topic: " + topic +
					" this may be over-ridden by event headers");
		}

		kafkaProps = KafkaSinkUtil.getKafkaProperties(context);

		if (logger.isDebugEnabled()) {
			logger.debug("Kafka producer properties: " + kafkaProps);
		}
	}
}

