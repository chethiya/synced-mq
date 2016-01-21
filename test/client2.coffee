MessageQueue = require './../mq'

opt =
 client: on
 host: 'localhost'
 port: 8022
mq = new MessageQueue opt
mq.onStarted ->
 mq.push {q: 1, v: 3}, ->
  mq.top (v) ->
   console.log v
   mq.move 'test2_q2', ->
    mq.top (v) ->
     console.log v
