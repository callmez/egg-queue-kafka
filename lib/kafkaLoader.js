'use strict';

const path = require('path');
const assert = require('assert');
const _ = require('lodash');
const kafka = require('kafka-node');
const { Kafka, Drainer } = require('sinek');

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

  async function createQueue(options) {
    const {
      logger = app.logger,
      producer,
      consumer,
      ...clientOptions
    } = options;
    const connectString = clientOptions.kafkaHost || clientOptions.connectionString;
    assert(connectString, 'kafkaHost or connectionString option of client muse be set.');
    const connectDirectlyToBroker = !!clientOptions.kafkaHost; // 优先 broker
    const clientId = connectDirectlyToBroker ? undefined : clientOptions.clientId;

    { // consumer
      const { topics, drainer, ...consumerOptions } = consumer;
      const KafkaConsumer = new Kafka(connectString, clientId, connectDirectlyToBroker);
      KafkaConsumer.becomeConsumer(topics, undefined, consumerOptions);
      attachConsumerListeners(consumer, drainer);
    }

    { // producer
      const { topics, ...producerOptions } = producer;
      // @see https://github.com/SOHU-Co/kafka-node/issues/354
      assert(topics.length, "producer must set target 'topics' for send method.");
      const KafkaProducer = new Kafka(connectString, clientId, connectDirectlyToBroker);
      KafkaProducer.becomeProducer(topics, clientId, producerOptions);
      KafkaProducer.logger = logger;
      attachProducerListeners(producer);
      return KafkaProducer;
    }
  }

  async function attachConsumerListeners(consumer, drainerOptions = {}) {
    const { asyncLimit = 5, autoJsonParsing, omitQueue, commitOnDrain } = drainerOptions;
    const Consumer = new Drainer(consumer, asyncLimit, autoJsonParsing, omitQueue, commitOnDrain);
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

  async function attachProducerListeners(producer) {
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

