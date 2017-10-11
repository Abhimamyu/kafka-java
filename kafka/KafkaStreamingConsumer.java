import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 */

/**
 * @author abhimanyu
 *
 */
public class Stream implements Runnable {
	private static int jobId = 0;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private KafkaConsumer<String, String> consumer;

	@Override
	public void run() {

		List<String> topic = setParameter();

		kafkaConsumerRun(topic);

	}

	/**
	 * @param topic
	 * @throws WakeupException
	 */
	private void kafkaConsumerRun(List<String> topic) throws WakeupException {
		System.out.println("kafka consumer server started.......");
		try {
			consumer.subscribe(topic);
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(10000);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(record.value());
				}
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	/**
	 * @return
	 */
	private List<String> setParameter() {
		List<String> topic = Arrays.asList("test", "test-old");
		String group = "test-consumer-group";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", group);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
		System.out.println("kafka consumer parameters.......");
		return topic;
	}

	public static void main(String[] arg) {
		Stream stream = new Stream();
		System.out.println("kafka consumer server starting.......");
		stream.run();
	}

}
