MessageQueue = require './../mq'

opt =
 client: on
 host: 'localhost'
 port: 8022
mq = new MessageQueue opt
mq.onStarted ->
 mq.push {time: (new Date).getTime()}, ->
  mq.top (data) ->
   console.log data
   mq.pop ->
    console.log 'popped'
