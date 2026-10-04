// Dev-only one-shot tap: reads the next OTP_EMAIL message off Kafka.
const { Kafka } = require('kafkajs');
const TOPIC = 'notification.otp-email';

const kafka = new Kafka({ clientId: 'otp-tap', brokers: ['127.0.0.1:9093'] });
const consumer = kafka.consumer({ groupId: 'otp-tap-' + process.argv[2] });

(async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const v = JSON.parse(message.value.toString());
      console.log('OTP_EMAIL_PAYLOAD=' + JSON.stringify(v));
      await consumer.disconnect();
      process.exit(0);
    },
  });
  setTimeout(() => { console.error('TIMEOUT: no OTP message seen'); process.exit(2); }, 25000);
})().catch((e) => { console.error(e.message); process.exit(1); });
