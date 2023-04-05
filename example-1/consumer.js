const { Kafka } = require("kafkajs");

const topic_name = process.argv[2];

createConsumer();

async function createConsumer() {
    try {
        // Admin Stuff
        const kafka = new Kafka({
            clientId: "kafka_example_1",
            brokers: ["192.168.1.38:9092"],
        });

        const consumer = kafka.consumer({
            groupId: "example_1_consumerGroup_1"
        });
        console.log("Connecting to consumer...");

        await consumer.connect();
        console.log("Connected to consumer!");

        await consumer.subscribe({
            topic: topic_name,
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async (result) => {
                console.log(`Received message: ${result.message.value} on partition: ${result.partition}`);
            }
        })
    } catch (error) {
        console.log("Error: ", error);
    }
}
