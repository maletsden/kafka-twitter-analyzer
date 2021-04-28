import express from 'express';
import {Kafka, logLevel} from "kafkajs";
import KafkaConsumer from "./src/kafka/KafkaConsumer.js";
import Deque from "collections/deque.js";
import {Storage} from '@google-cloud/storage';

const app = express();

const clientId = "my-app";
const brokers = process.env.KAFKA_BROKERS.split(',');
const topic = "twitter-tweets";
const partitions = [0, 1, 2];

const storage = new Storage();
const bucketName = 'twitter-reports';
const bucket = storage.bucket(bucketName);

/**
 * Update hour for current Date object
 *
 * @param {int} hours
 * @param {boolean} subtract
 *
 * @return {Date}
 */
Date.prototype.updateHour = function (hours, subtract = true) {
    this.setHours(this.getHours() - (subtract ? hours : -hours));
    return this;
};

/**
 * Twitter tweets data saved in each Kafka message
 *
 * @typedef {{
 *   target: int,
 *   id: int,
 *   date: string,
 *   flag: string,
 *   user: string,
 *   text: string,
 * }} TweetData
 */

/**
 * Starts consumer for
 *
 * @param {string} entryPointName
 * @param {Express.Response} res
 * @param {string} topic
 * @param {number} timestamp
 * @param {Function} messageHandler
 * @param {Function} onSuccessCallback
 *
 * @return {Promise<void>}
 */
async function getAllMessagesFromTimestamp(
    entryPointName, res, topic, timestamp, messageHandler, onSuccessCallback
) {
    const kafkaClient = new Kafka({clientId, brokers});
    const kafkaConsumer = new KafkaConsumer(kafkaClient, clientId);

    try {
        // set offsets to specific timestamp
        const partitionsFromTimestamp = await kafkaConsumer.fetchTopicOffsetsByTimestamp(topic, timestamp);
        await kafkaConsumer.setOffsets(topic, partitionsFromTimestamp);
        // get all messages
        await kafkaConsumer.getAllMessages(topic, messageHandler, () => onSuccessCallback(kafkaConsumer));
        console.log(`KafkaConsumer started getting ${entryPointName}...`);
    } catch (err) {
        await kafkaConsumer.disconnect();
        console.error(`ERROR: KafkaConsumer getting ${entryPointName}:`, err);
        res.send('Error during executing request');
    }
}

/**
 * Saves report to Google Cloud Bucket
 *
 * @param {string} reportName
 * @param {string} data
 *
 * @return {Promise<void>}
 */
async function saveReportToBucket(reportName, data) {
    const file = bucket.file(reportName);

    try {
        await file.save(data);
        console.log('File written successfully.');
    } catch (err) {
        console.error('ERROR:', err);
    }
}

/**
 * Get last `n` hours from query
 *
 * @param {Object} query
 *
 * @return {number}
 */
function getLastNHours(query) {
    let {last_n_hours = 3} = query;
    try {
        last_n_hours = typeof last_n_hours !== "number" ? parseInt(last_n_hours) : last_n_hours;
    } catch (_) {
        last_n_hours = 0;
    }
    return Math.max(last_n_hours, 0);
}

/**
 * Format Date if form 'y-m-d-h-m-s'
 *
 * @param {int | string | Date} date
 *
 * @return {string}
 */
function formatDate(date) {
    const d = new Date(date);
    let month = d.getMonth() + 1,
        day = d.getDate(),
        year = d.getFullYear(),
        hour = d.getHours(),
        minute = d.getMinutes(),
        second = d.getSeconds();

    return [year, month, day, hour, minute, second].map((val) => val.toString().length < 2 ? '0' + val : val).join('-');
}

/**
 * Show the list of all the accounts that you have tweets from
 */
app.get('/all-users', async (req, res) => {
    const kafkaClient = new Kafka({clientId, brokers});
    const kafkaConsumer = new KafkaConsumer(kafkaClient, clientId);

    /**
     * Set of all usernames
     *
     * @type {Set<string>}
     */
    const users = new Set();

    /**
     * Gets all usernames for each tweet
     *
     * @param {Object} message
     */
    const messageHandler = ({message}) => {
        /**
         * @type {TweetData}
         */
        const tweet = JSON.parse(message.value);
        users.add(tweet.user);
    };

    /**
     * Successfully finish after reading all messages
     *
     * @return {Promise<void>}
     */
    const onSuccessCallback = async () => {
        await kafkaConsumer.disconnect();
        console.log('KafkaConsumer finished getting all users.');
        const data = {
            users: Array.from(users),
        };
        await saveReportToBucket(`report-all-users-${formatDate(Date.now())}.json`, JSON.stringify(data));
        res.json(data);
    };

    try {
        // set offsets for all partitions to beginning
        const partitionsFromBegging = partitions.map((partition) => ({partition, offset: '-2'}));
        await kafkaConsumer.setOffsets(topic, partitionsFromBegging);
        // read all messages
        await kafkaConsumer.getAllMessages(topic, messageHandler, onSuccessCallback);
        console.log('KafkaConsumer started getting all users...');
    } catch (err) {
        await kafkaConsumer.disconnect();
        console.error('ERROR: KafkaConsumer getting all users:', err);
        res.send('Error during executing request');
    }
});


/**
 * Show the aggregated statistics with a number of tweets per each hour for all the accounts (for the last 3 hours).
 */
app.get('/tweets-number-per-hour', async (req, res) => {
    const entryPointName = 'tweets number for each hour';
    const last_n_hours = getLastNHours(req.query);

    const timestamps = new Array(last_n_hours + 1).fill(0);
    for (let i = 0; i < last_n_hours; ++i) {
        timestamps[i] = new Date().updateHour(last_n_hours - i).getTime();
    }
    timestamps[timestamps.length - 1] = Date.now();
    const timestamp = timestamps[0];

    /**
     * Tweets number for each past hour
     *
     * @type {Array}
     */
    const tweetsNumByHour = new Array(timestamps.length - 1).fill(0);

    /**
     * Gets last tweets ant their number for each user
     *
     * @param {Object} message
     */
    const messageHandler = ({message}) => {
        const {timestamp} = message;
        for (let i = 0; i < timestamps.length - 1; ++i) {
            if (timestamps[i] <= timestamp && timestamp < timestamps[i + 1]) {
                tweetsNumByHour[i]++;
                break;
            }
        }
    };

    const onSuccessCallback = async (kafkaConsumer) => {
        await kafkaConsumer.disconnect();
        console.log(`KafkaConsumer finished getting ${entryPointName}.`);
        const reportName =
            `report-tweets-number-per-hour-last_n_hours-${last_n_hours}-${formatDate(Date.now())}.json`;
        await saveReportToBucket(reportName, JSON.stringify(tweetsNumByHour));
        res.json(tweetsNumByHour);
    };

    await getAllMessagesFromTimestamp(
        entryPointName, res, topic, timestamp, messageHandler, onSuccessCallback
    );
});


/**
 * Show the last 10 tweets for each of the 10 accounts which posted the highest number of tweets in the last 3 hours.
 */
app.get('/last-tweets-from-most-productive', async (req, res) => {
    const entryPointName = 'most productive user\' last tweets';
    const last_n_hours = getLastNHours(req.query);

    const timestamp = new Date().updateHour(last_n_hours).getTime();

    const MAX_TWEETS_NUM = 10;
    const MAX_MOST_PRODUCTIVE_USERS_NUM = 10;

    /**
     * Tweets number for each user
     *
     * @type {Map<string, int>}
     */
    const tweetsNumByUsers = new Map();
    /**
     * Last Tweets for each user
     *
     * @type {Map<string, Deque>}
     */
    const lastTweetsByUsers = new Map();

    /**
     * Gets last tweets ant their number for each user
     *
     * @param {Object} message
     */
    const messageHandler = ({message}) => {
        /**
         * @type {TweetData}
         */
        const tweet = JSON.parse(message.value);
        const user = tweet.user;

        // save tweets number
        tweetsNumByUsers.set(user, (tweetsNumByUsers.get(user) || 0) + 1);

        // Save last tweets
        const last_tweets = lastTweetsByUsers.get(user) || new Deque();
        lastTweetsByUsers.set(user, last_tweets);
        last_tweets.add(tweet.text);
        if (last_tweets.length > MAX_TWEETS_NUM) last_tweets.shift();
    };

    const onSuccessCallback = async (kafkaConsumer) => {
        await kafkaConsumer.disconnect();
        console.log(`KafkaConsumer finished getting ${entryPointName}.`);

        const mostProductiveUsers = Array.from(tweetsNumByUsers)
            .sort((a, b) => b[1] - a[1])
            .slice(0, MAX_MOST_PRODUCTIVE_USERS_NUM);

        const lastTweets = mostProductiveUsers.map(
            ([user]) => ({user, tweets: lastTweetsByUsers.get(user).toArray()})
        );

        const reportName = `report-last-tweets-from-most-productive-last_n_hours-${last_n_hours}-${formatDate(Date.now())}.json`;
        await saveReportToBucket(reportName, JSON.stringify(lastTweets));

        res.json(lastTweets);
    };

    await getAllMessagesFromTimestamp(
        entryPointName, res, topic, timestamp, messageHandler, onSuccessCallback
    );
});


/**
 * Show Top 20 most “tweet-producing” accounts for the last n hours (n should be given as a param).
 */
app.get('/most-productive-users', async (req, res) => {
    const entryPointName = 'most productive users';
    const last_n_hours = getLastNHours(req.query);

    const timestamp = new Date().updateHour(last_n_hours).getTime();

    const MAX_MOST_PRODUCTIVE_USERS_NUM = 20;

    /**
     * Tweets number for each user
     *
     * @type {Map<string, int>}
     */
    const tweetsNumByUsers = new Map();

    /**
     * Counts all tweets for each user
     *
     * @param {Object} message
     */
    const messageHandler = ({message}) => {
        /**
         * @type {TweetData}
         */
        const tweet = JSON.parse(message.value);
        const user = tweet.user;

        // save tweets number
        tweetsNumByUsers.set(user, (tweetsNumByUsers.get(user) || 0) + 1);
    };

    const onSuccessCallback = async (kafkaConsumer) => {
        await kafkaConsumer.disconnect();
        console.log('KafkaConsumer finished getting all users.');

        const mostProductiveUsers = Array.from(tweetsNumByUsers)
            .sort((a, b) => b[1] - a[1])
            .slice(0, MAX_MOST_PRODUCTIVE_USERS_NUM)
            .map(([user]) => user);

        const data = {users: mostProductiveUsers};
        const reportName = `report-most-productive-users-last_n_hours-${last_n_hours}-${formatDate(Date.now())}.json`;
        await saveReportToBucket(reportName, JSON.stringify(data));

        res.json(data);
    };

    await getAllMessagesFromTimestamp(
        entryPointName, res, topic, timestamp, messageHandler, onSuccessCallback
    );
});


/**
 * Show the most popular hashtags across all the tweets during the last n hours (n should be given as a param)
 */
app.get('/most-popular-hashtags', async (req, res) => {
    const entryPointName = 'most popular hashtags';
    const last_n_hours = getLastNHours(req.query);

    const timestamp = new Date().updateHour(last_n_hours).getTime();

    const MAX_MOST_POPULAR_HASHTAGS_NUM = 20;

    const kafkaClient = new Kafka({clientId, brokers});
    const kafkaConsumer = new KafkaConsumer(kafkaClient, clientId);

    /**
     * Tweets number for each user
     *
     * @type {Map<string, int>}
     */
    const numUsageByHashtags = new Map();

    /**
     * Count all hashtags in each message
     *
     * @param {Object} message
     */
    const messageHandler = ({message}) => {
        /**
         * @type {TweetData}
         */
        const tweet = JSON.parse(message.value);
        const regexp = /\B#\w\w+\b/g;
        const hashtags = tweet.text.match(regexp) || [];
        hashtags.forEach(
            (hashtag) => numUsageByHashtags.set(hashtag, (numUsageByHashtags.get(hashtag) || 0) + 1)
        );
    };

    /**
     * Successfully send report
     *
     * @return {Promise<void>}
     */
    const onSuccessCallback = async () => {
        await kafkaConsumer.disconnect();
        console.log(`KafkaConsumer finished getting ${entryPointName}.`);

        const mostPopularHashtags = Array.from(numUsageByHashtags)
            .sort((a, b) => b[1] - a[1])
            .slice(0, MAX_MOST_POPULAR_HASHTAGS_NUM)
            .map(([hashtag]) => hashtag);

        const data = {hashtags: mostPopularHashtags};
        const reportName = `report-most-popular-hashtags-last_n_hours-${last_n_hours}-${formatDate(Date.now())}.json`;
        await saveReportToBucket(reportName, JSON.stringify(data));

        res.json(data);
    };

    await getAllMessagesFromTimestamp(
        entryPointName, res, topic, timestamp, messageHandler, onSuccessCallback
    );
});


const server = app.listen(3000, () => {
    console.log('Example app listening on port 3000!')
});