const { Kafka } = require("kafkajs");

const topic_name = process.argv[2];
const partition = process.argv[3];

createProducer();

async function createProducer() {
    try {
        // Admin Stuff
        const kafka = new Kafka({
            clientId: "kafka_example_1",
            brokers: ["192.168.1.38:9092"],
        });

        const producer = kafka.producer();
        console.log("Connecting to producer...");

        await producer.connect();
        console.log("Connected to producer!");

        const message_result = await producer.send({
            topic: topic_name,
            messages: [
                {
                    value: "This is the test log message.",
                    partition: partition
                }
            ]
        });
        console.log("Message sent: ", JSON.stringify(message_result));

        await producer.disconnect();
    } catch (error) {
        console.log("Error: ", error);
    } finally {
        process.exit(0);
    }
}
