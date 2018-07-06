'use strict';

const path = require('path');
const assert = require('assert');
const _ = require('lodash');
const kafka = require('kafka-node');

module.exports = app => {
  const config = app.config.queue;

  if (!app.Kafka) app.Kafka = kafka;
  app.addSingleton('queue', createQueue);

  loadQueue(config.loadOptions || {});

  function loadQueue(options) {
    const { loader } = app;

    loader.timing.start('Load Queue');
    // 载入到 app.queueClasses
    options = Object.assign({
      call: true,
      caseStyle: 'lower',
      fieldClass: 'queueClasses',
      directory: path.join(app.options.baseDir, 'app/queue'),
    }, options);
    loader.loadToContext(options.directory, 'queue', options);
    loader.timing.end('Load Queue');
  }

  function createQueue(options) {
    const { producer: { topic, ...producerOptions }, consumers = [], ...clientOptions } = options;
    assert(clientOptions.kafkaHost, 'kafkaHost option of client muse be set.');
    assert(topic, "producer must set a default topic for send method.");
    assert(consumers.length, 'consumers must set at least one');

    const client = new kafka.KafkaClient(clientOptions);

    for (const { topics, ...consumerOptions } of consumers) {
      const Consumer = new kafka.ConsumerGroup({ kafkaHost: clientOptions.kafkaHost, ...consumerOptions }, topics);
      attachConsumerEvent(Consumer);
    }

    const Producer = new kafka.Producer(client, producerOptions);

    Producer.topic = topic;

    attachProducerEvent(Producer);
    return Producer;
  }

  async function attachConsumerEvent(consumer) {
    consumer.on('error', err => {
      throw new Error(err);
    });
    consumer.on('offsetOutOfRange', err => {
      throw new Error(err);
    });
    consumer.on('message', message => {
      const { key } = message;
      return getQueue(key).processQueue(message);
    });
  }

  async function attachProducerEvent(producer) {
    return new Promise((resolve, reject) => {
      producer.on('error', err => {
        throw new Error(err);
      });
      producer.on('ready', resolve);
    });
  }

  let ctx;
  function getContext() {
    if (!ctx) ctx = app.createAnonymousContext();
    return ctx;
  }

  function getQueue(key) {
    const queue = _.get(getContext(), key);
    if (!queue) throw new Error(`The queue ${queue} is not exists.`);
    return queue;
  }
};

