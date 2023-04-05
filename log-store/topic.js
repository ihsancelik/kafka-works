const { Kafka } = require("kafkajs");

createTopic();

async function createTopic() {
  try {
    // Admin Stuff
    const kafka = new Kafka({
      clientId: "kafka_log_store_client",
      brokers: ["192.168.1.38:9092"],
    });

    const admin = kafka.admin();
    console.log("Connecting to kafka broker...");

    await admin.connect();
    console.log("Connected!");

    await admin.createTopics({
      topics: [
        { topic: "LogStoreTopic", numPartitions: 2 }
      ],
    });
    console.log("LogStoreTopic created!");

    await admin.disconnect();
  } catch (error) {
    console.log("Error: ", error);
  } finally {
    process.exit(0);
  }
}
