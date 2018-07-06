'use strict';

module.exports = app => {
  require('./lib/kafkaLoader')(app);
};

