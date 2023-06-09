const { Kafka } = require("kafkajs");

const log_data = require("./system_logs.json");

createProducer();

async function createProducer() {
    try {
        // Admin Stuff
        const kafka = new Kafka({
            clientId: "kafka_log_store_client",
            brokers: ["192.168.1.38:9092"],
        });

        const producer = kafka.producer();
        console.log("Connecting to producer...");

        await producer.connect();
        console.log("Connected to producer!");

        let messages = log_data.map(item => {
            return {
                value: JSON.stringify(item),
                partition: item.type == "system" ? 0 : 1
            }
        });

        const message_result = await producer.send({
            topic: "LogStoreTopic",
            messages: messages
        });
        console.log("Message sent: ", JSON.stringify(message_result));

        await producer.disconnect();
    } catch (error) {
        console.log("Error: ", error);
    } finally {
        process.exit(0);
    }
}
