# getting-started-kafka

docker compose -p demo3 up -d
docker stop broker-2
docker logs broker-1

#  command
vim ~/.zshrc
export PATH="/Users/abdeldjalilmaiza/tools/kafka_2.13-4.1.1/bin:$PATH"

#  topics command
kafka-topics.sh --create --bootstrap-server 127.0.0.1:9092 --replication-factor 3 --partitions 3 --topic myorders
kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 2 --topic myorders
kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 4 --topic connectlog
kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 4 --topic connect-distributed
kafka-topics.sh --create --bootstrap-server 127.0.0.1:9092 --partitions 2 --topic maiza
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --delete --topic myorders

kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 4 --topic rawTempReadings
kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 4 --topic validatedTempReadings

kafka-topics.sh --create --bootstrap-server localhost:9092 --topic kafka_connect_statuses --config cleanup.policy=compact
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic kafka_connect_configs --config cleanup.policy=compact
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic kafka_connect_offsets --config cleanup.policy=compact

afka-reassign-partitions.sh --bootstrap-server 127.0.0.1:9092 --reassignment-json-file increase_replication.json --execute

#  producer command
kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic
kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic myorders --property "parse.key=true" --property "key.separator=:"

#  consumer command
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic myorders --from-beginning
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic myorders --group 1

kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic myorders --from-beginning --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.DoubleDeserializer --property print.key=tue --property key.separator=, --group 1
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic myorders --from-beginning --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.DoubleDeserializer --property print.key=tue --property key.separator=, --group 1

#  consumer group command
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --list
kafka-consumer-groups.sh --all-groups --all-topics --bootstrap-server 127.0.0.1:9092 --describe
kafka-consumer-groups.sh --all-groups --all-topics --bootstrap-server 127.0.0.1:9092 --describe
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group 1
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --delete --group 1

#  maven command
mvn clean install exec:java -Dexec.mainClass="com.globomantics.Consumer" -Dexec.args="1"

#  connector command
connect-standalone.sh worker.properties filesink.properties
connect-distributed.sh worker.properties

confluent-hub install confluentinc/kafka-connect-avro-converter:7.5.0 --component-dir ~/tools/kafka_2.13-4.1.1/libs --worker-configs worker.properties
confluent-hub install mongodb/kafka-connect-mongodb:1.11.0 --component-dir ~/tools/kafka_2.13-4.1.1/libs --worker-configs worker.properties

exécuter le vrai script dans Caskroom
/opt/homebrew/Caskroom/confluent-hub-client/7.3.4/bin/confluent-hub install confluentinc/kafka-connect-avro-converter:7.5.0 --component-dir ~/tools/kafka_2.13-4.1.1/libs --worker-configs worker.properties
/opt/homebrew/Caskroom/confluent-hub-client/7.3.4/bin/confluent-hub install mongodb/kafka-connect-mongodb:1.11.0 --component-dir ~/tools/kafka_2.13-4.1.1/libs --worker-configs worker.properties

# Homebrew
Homebrew installe des outils via des formules hébergées dans des repositories Git appelés taps
brew tap = “Ajoute un dépôt externe de formules Homebrew”
brew tap confluentinc/homebrew-confluent-hub-client

brew install --cask confluent-hub-client
install le Confluent Hub Client sur ta machine, via Homebrew, en utilisant un cask
    Pourquoi --cask ?
    Formula vs Cask (rapide)
    formula → outils CLI compilés (binaires, libs)
    cask → applications / binaires précompilés distribués tels quels

# Ksql
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
SET 'auto.offset.reset'='earliest';
CREATE STREAM tempReading (zipcode VARCHAR, sensortime BIGINT, temp DOUBLE) WITH (kafka_topic='readings', timestamp='sensortime', value_format='json', partitions=1);
show stream extended;
INSERT INTO tempReading (zipcode, sensortime, temp) VALUES ('1865', UNIX_TIMESTAMP(), 20);
INSERT INTO tempReading (zipcode, sensortime, temp) VALUES ('1806', UNIX_TIMESTAMP(), 25);
INSERT INTO tempReading (zipcode, sensortime, temp) VALUES ('1865', UNIX_TIMESTAMP() + 60* 60 * 1000, 32);
INSERT INTO tempReading (zipcode, sensortime, temp) VALUES ('1806', UNIX_TIMESTAMP() + 60* 60 * 1000, 27);

SELECT * FROM tempReading EMIT CHANGES;

SELECT zipcode, TIMESTAMPTOSTRING(WINDOWSTART, 'HH:mm:ss') as windowtime, COUNT(*) AS rowcount , AVG(temp) as temp FROM tempReading WINDOW TUMBLING (SIZE 1 HOURS) GROUP BY zipcode EMIT CHANGES;
CREATE TABLE highsandlows WITH (kafka_topic='reading') AS SELECT MIN(temp) as min_temp, MAX(temp) as max_temp, zipcode FROM tempReading GROUP BY zipcode;
SELECT min_temp, max_temp, zipcode FROM highsandlows WHERE zipcode = '1865';

INSERT INTO tempReading (zipcode, sensortime, temp) VALUES ('1865', UNIX_TIMESTAMP() + 60* 60 * 1000, 35);

