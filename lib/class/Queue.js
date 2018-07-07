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
   * @param {any} messages -
   * @param {any} options -
   * @return {Promise<*>} -
   */
  async add(messages, options = {}) {
    return this.addQueue(messages, {
      ...this.options,
      ...options,
    });
  }

  /**
   * 执行任务. 复写该方法执行任务流程
   * @param {any} message -
   * @param {object} data -
   * @return {Promise<*>}
   */
  async process(message, data) {
    return this.app.logger.error('process method must be override');
  }

  /**
   * 调用子任务
   * @param string name
   * @param {Array} args
   * @return {Promise<*>}
   */
  async addSubQueue(name, ...args) {
    const queue = _.get(this.ctx.queue, name);
    if (!queue instanceof Queue) {
      throw new Error('The sub queue class must instance of Queue');
    }
    const result = await queue.add.apply(queue, args);
    return result;
  }

  /**
   * 该方法为基础调用. 请勿复写
   * ```
   * addQueue('a'); // add single message
   * addQueue([ 'a', 'b' ]) // add multi messages
   * addQueue({ a: 1 }) // add single object message
   * addQueue([ { a: 1 }, { b: 2 } ]) // add multi object messages
   * ```
   * @param {any} messages -
   * @param {any} options -
   * @return {Promise<*>} -
   */
  async addQueue(messages = {}, options = {}) {
    const { queue, pathName } = this;

    messages = (Array.isArray(messages) ? messages : [ messages ])
      .map(message => JSON.stringify(message)); // JSON的序列化

    const payload = {
      key: pathName,
      topic: queue.topic,
      ...options,
      messages,
    };

    const payloads = [ payload ];

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
    const result = await this.process(message.value, message);
    return result;
  }
};

module.exports = Queue;
