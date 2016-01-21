MessageQueue = require './../mq'

opt =
 client: on
 host: 'localhost'
 port: 8022
mq = new MessageQueue opt

push = (N, cb) ->
 n = N
 next = ->
  if n is 0
   cb()
   return
  n--
  mq.push {n: N, i: N-n}, ->
   console.log 'push', N-n
   next()
 next()

top = (N, cb) ->
 n = N
 next = ->
  if n is 0
   cb()
   return
  n--
  mq.top (val) ->
   console.log 'top', val
   next()
 next()

pop = (N, cb) ->
 n = N
 next = ->
  if n is 0
   cb()
   return
  n--
  mq.pop (val) ->
   console.log 'pop', val
   next()
 next()

mq.onStarted ->
 pop 3, ->
  top 3, ->
   push 1, ->
    top 3, ->
     pop 3, ->
      top 3, ->
       console.log 'done'
