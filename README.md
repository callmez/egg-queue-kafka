```js
// config.default.js
  config.queue = {
    client: {
      kafkaHost: 'localhost:9092',
      producer: {
        topics: [ 'task' ],
      },
      consumer: {
        topics: [ 'task' ]
      },
    }
  };
```