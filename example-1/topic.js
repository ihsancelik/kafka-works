const { Kafka } = require("kafkajs");

createTopic();

async function createTopic() {
  try {
    // Admin Stuff
    const kafka = new Kafka({
      clientId: "kafka_example_1",
      brokers: ["192.168.1.38:9092"],
    });

    const admin = kafka.admin();
    console.log("Connecting to kafka broker...");

    await admin.connect();
    console.log("Connected!");

    await admin.createTopics({
      topics: [
        { topic: "Logs", numPartitions: 1 },
        { topic: "Logs2", numPartitions: 2 },
      ],
    });
    console.log("Topic created!");

    await admin.disconnect();
  } catch (error) {
    console.log("Error: ", error);
  } finally {
    process.exit(0);
  }
}
