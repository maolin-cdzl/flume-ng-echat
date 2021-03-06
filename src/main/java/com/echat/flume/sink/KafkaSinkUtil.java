package com.echat.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.PropertiesTrait;

import java.util.Map;
import java.util.Properties;

public class KafkaSinkUtil {

  private static final Logger log =
          LoggerFactory.getLogger(KafkaSinkUtil.class);

  public static Properties getKafkaProperties(Context context) {
    log.info("context={}",context.toString());
    Properties props =  generateDefaultKafkaProps();
    setKafkaProps(context, props);
    addDocumentedKafkaProps(context, props);
    return props;
  }

  /**
   * Some of the producer properties are especially important
   * We documented them and gave them a camel-case name to match Flume config
   * If user set these, we will override any existing parameters with these
   * settings.
   * Knowledge of which properties are documented is maintained here for now.
   * If this will become a maintenance issue we'll set a proper data structure.
   */
  private static void addDocumentedKafkaProps(Context context,
                                              Properties kafkaProps)
          throws ConfigurationException {
    String brokerList = context.getString(KafkaSinkConstants
            .BROKER_LIST_FLUME_KEY);
    if (brokerList == null) {
      throw new ConfigurationException("brokerList must contain at least " +
              "one Kafka broker");
    }
    kafkaProps.put(KafkaSinkConstants.BROKER_LIST_KEY, brokerList);

    String requiredKey = context.getString(
            KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY);

    if (requiredKey != null ) {
      kafkaProps.put(KafkaSinkConstants.REQUIRED_ACKS_KEY, requiredKey);
    }
  }


  /**
   * Generate producer properties object with some defaults
   * @return
   */
  private static Properties generateDefaultKafkaProps() {
    Properties props = new Properties();
    props.put(KafkaSinkConstants.MESSAGE_SERIALIZER_KEY,
            KafkaSinkConstants.DEFAULT_MESSAGE_SERIALIZER);
    props.put(KafkaSinkConstants.KEY_SERIALIZER_KEY,
            KafkaSinkConstants.DEFAULT_KEY_SERIALIZER);
    props.put(KafkaSinkConstants.REQUIRED_ACKS_KEY,
            KafkaSinkConstants.DEFAULT_REQUIRED_ACKS);
    return props;
  }


  /**
   * Add all configuration parameters starting with "kafka"
   * to producer properties
   */
  private static void setKafkaProps(Context context, Properties kafkaProps) {

    Map<String,String> kafkaProperties =
            context.getSubProperties(KafkaSinkConstants.PROPERTY_PREFIX);

    for (Map.Entry<String,String> prop : kafkaProperties.entrySet()) {

      kafkaProps.put(prop.getKey(), prop.getValue());
      if (log.isDebugEnabled()) {
        log.debug("Reading a Kafka Producer Property: key: "
                + prop.getKey() + ", value: " + prop.getValue());
      }
    }
  }
}

