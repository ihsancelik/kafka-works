const { Kafka } = require("kafkajs");

const groupId = process.argv[2];

createConsumer();

async function createConsumer() {
    try {
        // Admin Stuff
        const kafka = new Kafka({
            clientId: "kafka_pub_sub_client",
            brokers: ["192.168.1.38:9092"],
        });

        const consumer = kafka.consumer({
            groupId: `mobile_encoder_consumer_group_${groupId}`
        });
        console.log("Connecting to consumer...");

        await consumer.connect();
        console.log("Connected to consumer!");

        await consumer.subscribe({
            topic: "raw_video_topic",
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async (result) => {
                console.log(`Received message: ${result.message.value} on partition: ${result.partition} groupId:${groupId}`);
            }
        })
    } catch (error) {
        console.log("Error: ", error);
    }
}
