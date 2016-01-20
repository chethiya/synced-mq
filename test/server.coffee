MessageQueue = require './../mq'

opt =
 id: 'testq'
 port: 8022
 filePath: './q.json'
 syncFile: './sync.log'

mq = new MessageQueue opt
mq.onStarted ->
 mq.top (data) ->
  console.log  data
  mq.top (data) ->
   console.log data
