const { Kafka } = require("kafkajs");

createProducer();

async function createProducer() {
    try {
        // Admin Stuff
        const kafka = new Kafka({
            clientId: "kafka_pub_sub_client",
            brokers: ["192.168.1.38:9092"],
        });

        const producer = kafka.producer();
        console.log("Connecting to producer...");

        await producer.connect();
        console.log("Connected to producer!");

        const message_result = await producer.send({
            topic: "raw_video_topic",
            messages: [
                { value: "New Video Content!", partition: 0 }
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