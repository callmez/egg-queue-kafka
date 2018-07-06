'use strict';

const _ = require('lodash');
const { BaseContextClass } = require('egg');
const Singleton = require('egg/lib/core/singleton');

const QUEUE = Symbol('Task#queue')

class Queue extends BaseContextClass {

  /**
   * 通过复写该方法获得该task默认选项
   * @return {{}} - return task options
   */
  get options() {
    return {
    };
  }

  /**
   * 获取指定queue处理任务
   * @return {*}
   */
  get queue() {
    if (!this[QUEUE]) {
      const { app: { queue } } = this;
      let client;
      if (queue instanceof Singleton) { // 多queue环境
        // 通过指定queueName可以为当前task指定queue, 必须为存在的queue.name;
        const name = this.queueName || queue.clients.keys().next().value; // 无指定则返回第一个queue
        client = queue.get(name);
        if (!client) throw new Error(`the name "${name}" of queue is not exists.`);
      } else {
        client = queue;
      }
      this[QUEUE] = client;
    }
    return this[QUEUE];
  }

  /**
   * 添加任务. 默认执行该操作,可复写更改任务流程
   * @param {any} data -
   * @return {Promise<*>} -
   */
  async add(data) {
    return this.addQueue(data);
  }

  /**
   * 执行任务. 复写该方法执行任务流程
   * @param {object} message -
   * @return {Promise<*>}
   */
  async process(message) {
    return this.app.logger.error('process method must be override');
  }

  /**
   * 该方法为基础调用. 请勿复写
   * ```
   * addQueue('a'); // add single message
   * addQueue([ 'a', 'b' ]) // add multi messages
   * addQueue({ a: 1 }) // add single object message
   * addQueue([ { a: 1 }, { b: 2 } ]) // add multi object messages
   * addQueue({ messages: { a: 1 }, ... }) // add single message with option
   * addQueue({ messages: [ { a: 1 }, { b: 2 } ], ... }) // add multi message with option
   * ```
   * @param {any} data -
   * @return {Promise<*>} -
   */
  async addQueue(data) {
    const { queue, options, pathName } = this;
    const payloads = Array.isArray(data) ? data : [ data ];

    for (const i in payloads) {
      let payload = payloads[i];
      if (typeof payload !== 'object' || !payload.hasOwnProperty('messages')) payload = { messages: payload };
      payload = {
        topic: queue.topic,
        ...options,
        ...payload,
        key: pathName,
      };
      if (!Array.isArray(payload.messages)) payload.messages = [ payload.messages ];
      payload.messages = payload.messages.map(message => JSON.stringify(message)); // JSON的序列化

      payloads[i] = payload;
    }

    return new Promise((resolve, reject) => {
      queue.send(payloads, (err, data) => {
        err ? reject(err) : resolve(data);
      });
    });
  }

  /**
   * 该方法为基础调用. 请勿复写
   * @param {object} message -
   * @return {Promise<*>} -
   */
  async processQueue(message) {
    message.value = JSON.parse(message.value); // JSON解序列
    const result = await this.process(message);
    return result;
  }
};

module.exports = Queue;
