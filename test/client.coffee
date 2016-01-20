MessageQueue = require './../mq'

opt =
 client: on
 host: 'localhost'
 port: 8022
mq = new MessageQueue opt
mq.onStarted ->
 mq.top (data) ->
  console.log  data
  mq.top (data) ->
   console.log data
