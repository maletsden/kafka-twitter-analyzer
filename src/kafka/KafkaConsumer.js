import {Kafka} from "kafkajs";

/**
 * KafkaConsumer - basic consumer for Kafka messages
 */
export default class KafkaConsumer {
    /**
     * @private
     *
     * KafkaJS Kafka Client
     *
     * @type {Kafka}
     */
    #kafkaClient

    /**
     * @private
     *
     * KafkaJS Kafka Client Id
     *
     * @type {string}
     */
    #clientId

    /**
     * @private
     *
     * KafkaJS Consumer
     *
     * @type {Object}
     */
    #consumer

    /**
     * @private
     *
     * KafkaJS Admin Client
     *
     * @type {Object}
     */
    #adminClient

    /**
     * Object that has partition number and offset for some Kafka topic
     *
     * @typedef {{
     *   partition: number,
     *   offset: string,
     * }} Kafka.SeekEntry
     */

    /**
     * Kafka EachMessagePayload
     *
     * @typedef {{
     *   topic: string,
     *   partition: number,
     *   message: Object,
     * }} Kafka.EachMessagePayload
     */

    /**
     * Handle all the messages that consumer consumed
     *
     * @callback consumerMessageHandler
     *
     * @param {Kafka.EachMessagePayload} payload
     */


    /**
     * Creates KafkaConsumer instance
     *
     * @param {Kafka} kafkaClient
     * @param {string} clientId
     */
    constructor(kafkaClient, clientId) {
        this.#kafkaClient = kafkaClient;
        this.#clientId = clientId;
        this.#consumer = kafkaClient.consumer({
            groupId: clientId,
            maxBytes: 10 * 1024 * 1024, // 10 MB
        });
        this.#adminClient = this.#kafkaClient.admin();
    }

    /**
     * Consumes all messages that are on portions and finishes
     *
     * @param {string} topic
     * @param {consumerMessageHandler} massageHandler
     * @param {Function} onSuccessCallback
     *
     * @return {Promise<void>}
     */
    async getAllMessages(topic, massageHandler, onSuccessCallback) {
        await this.#consumer.connect();

        /*
         * `consumedTopicPartitions` will be an object of all topic-partitions
         * and a boolean indicating whether or not we have consumed all
         * messages in that topic-partition. For example:
         *
         * {
         *   "topic-test-0": false,
         *   "topic-test-1": false,
         *   "topic-test-2": false
         * }
         */
        let consumedTopicPartitions = {};
        this.#consumer.on(this.#consumer.events.GROUP_JOIN, async ({payload}) => {
            const {memberAssignment} = payload;

            consumedTopicPartitions = Object.entries(memberAssignment).reduce(
                (topics, [topic, partitions]) => {
                    for (const partition in partitions) {
                        if (partitions.hasOwnProperty(partition)) {
                            topics[`${topic}-${partition}`] = false
                        }
                    }
                    return topics
                },
                {}
            )
        });

        /*
         * This is extremely unergonomic, but if we are currently caught up to the head
         * of all topic-partitions, we won't actually get any batches, which means we'll
         * never find out that we are actually caught up. So as a workaround, what we can do
         * is to check in `FETCH_START` if we have previously made a fetch without
         * processing any batches in between. If so, it means that we received empty
         * fetch responses, meaning there was no more data to fetch.
         *
         * We need to initially set this to true, or we would immediately exit.
         */
        let processedBatch = true;
        this.#consumer.on(this.#consumer.events.FETCH_START, async () => {
            if (!processedBatch) onSuccessCallback();

            processedBatch = false;
        });

        /*
         * Now whenever we have finished processing a batch, we'll update `consumedTopicPartitions`
         * and exit if all topic-partitions have been consumed,
         */
        this.#consumer.on(this.#consumer.events.END_BATCH_PROCESS, async ({payload}) => {
            const {topic, partition, offsetLag} = payload;
            consumedTopicPartitions[`${topic}-${partition}`] = offsetLag === '0';

            if (Object.values(consumedTopicPartitions).every((consumed) => Boolean(consumed))) {
                onSuccessCallback();
            }

            processedBatch = true;
        });

        await this.#consumer.subscribe({topic, fromBeginning: true});

        await this.#consumer.run({
            partitionsConsumedConcurrently: 3,
            eachMessage: massageHandler,
        });
    }


    /**
     * Set offsets for given topic
     *
     * @param {string} topic
     * @param {Array<Kafka.SeekEntry>} partitions
     *
     * @return {Promise<void>}
     */
    async setOffsets(topic, partitions) {
        await this.#adminClient.setOffsets({
            groupId: this.#clientId, topic, partitions,
        });
    }

    /**
     * Fetches topic offsets by timestamp
     *
     * @param {string} topic
     * @param {Integer} timestamp
     *
     * @return {Promise<Array<Kafka.SeekEntry>>}
     */
    async fetchTopicOffsetsByTimestamp(topic, timestamp) {
        return await this.#adminClient.fetchTopicOffsetsByTimestamp(topic, timestamp)
    }

    /**
     * Disconnect Consumer
     *
     * @return {Promise<void>}
     */
    async disconnect() {
        await this.#consumer.disconnect();
    }
}