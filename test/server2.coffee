MessageQueue = require './../mq'

targetId = 'test2_q2'
opt1 =
 id: 'test2_q1'
 port: 8022
 filePath: './test2_q1.json'
 syncFile: './test2_sync.log'

opt2 =
 id: targetId
 filePath: './test2_q2.json'
 syncFile: './test2_sync.log'

q1 = new MessageQueue opt1
q2 = new MessageQueue opt2

showTops = (cb) ->
 q1.top (v) ->
  console.log 'q1 top', v
  q2.top (v2) ->
   console.log 'q2 top', v2
   cb()

pullQ = (q, cb) ->
 console.log 'pulling q', q.id
 next = ->
  q.top (v) ->
   if not v?
    cb()
    return
   console.log 'pop: ', v
   q.pop next
 next()


q1.onStarted ->
 q2.onStarted ->
  q1.push {q:1, v: 1}, ->
   q1.push {q:1, v: 2}, ->
    q2.push {q:2, v: 1}, ->
     showTops ->
      q1.move targetId, ->
       showTops ->
        q1.move targetId, ->
         showTops ->
          q1.move targetId, ->
           showTops ->
            pullQ q1, ->
             pullQ q2, ->
              console.log 'done'

###
mq.onStarted ->
 mq.top (data) ->
  console.log  data
  mq.top (data) ->
   console.log data
###
