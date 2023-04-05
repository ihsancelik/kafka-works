const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
    try {
        // Admin Stuff
        const kafka = new Kafka({
            clientId: "kafka_log_store_client",
            brokers: ["192.168.1.38:9092"],
        });

        const consumer = kafka.consumer({
            groupId: "log_store_consumer_group"
        });
        console.log("Connecting to consumer...");

        await consumer.connect();
        console.log("Connected to consumer!");

        await consumer.subscribe({
            topic: "LogStoreTopic",
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
