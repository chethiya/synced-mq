Base = require './weya/base'
SyncedAppend = require './synced-append/synced-append'
IO = require './IO/io'
http = require 'http'
PATH = require 'path'
FS = require 'fs'

syncs = {}
queues = {}

CHUNK = 1<<17 # 128KB
SPACE = ' '.charCodeAt 0
NEW_LINE = '\n'.charCodeAt 0

class MessageQueue extends Base
 @extend()

 @initialize (opt) ->
  @startCB = []
  @started = off
  @cnt = 0

  if not opt?
   throw new Error "No options passed in for the message queue"

  @client = opt.client
  @client ?= off
  @onPushed = []

  if @client is off
   @id = opt.id
   if not @id?
    throw new Error "Missing ID in message queue"
   @filePath = opt.filePath
   if not @filePath?
    throw new Error "Missing filePath in message queue"
   @port = opt.port
   @syncFile = opt.syncFile
   if not @syncFile?
    throw new Error "syncFile is missing in server config"
   @syncFile = PATH.normalize @syncFile
   if syncs[@syncFile]?
    @sync = syncs[@syncFile]
   else
    @sync = syncs[@syncFile] = new SyncedAppend @syncFile
   queues[@id] = this

   files = {}
   files[@id] = @filePath
   @cursorPath = "#{@filePath}.cursor"
   files["#{@id}.cursor"] = @cursorPath
   @sync.syncStop()
   @sync.start files
   @qFile = @sync.getFile @id
   @cursorFile = @sync.getFile "#{@id}.cursor"

   @cursor = @nextCursor = @readCursor()
   @topValue = null
   @buffer = null

   if @port?
    server = new IO.ports.NodeHttpServerPort port: @port, http
    IO.addPort 'Client', server
    server.listen()
    @setupServer()

   @started = on
  else
   @host = opt.host
   @port = opt.port
   @host ?= 'localhost'
   if not @port?
    throw new Error "Port not given for the client mode message queue"

   options =
    host: @host
    port: @port
    path: '/'
   port = new IO.ports.NodeHttpPort options, http
   IO.addPort 'Server', port
   @started = on

 # files operations

 readCursor: ->
  try
   fd = FS.openSync @cursorPath, 'r'
   buffer = new Buffer CHUNK * 2
   pos = 0
   b = 0
   res = null
   while (cnt = FS.readSync fd, buffer, pos, CHUNK, b) > 0
    b += cnt
    i = pos
    end = pos + cnt
    while true
     while i < end and buffer[i] isnt SPACE
      i++
     if i < end
      res = parseInt buffer.toString 'utf8', pos, i
      i++
      pos = i
     if i >= end
      break
    buffer.copy buffer, 0, pos, end
    pos = end - pos
   return res
  catch
   return 0

 moveCursor: ->
  if @cursor is @nextCursor
   return false
  @cursor = @nextCursor
  @topValue = null
  @cursorFile.append "#{@cursor} "
  return true

 readTop: ->
  str = ''
  if not @buffer?
   try
    @fd = FS.openSync @filePath, 'r'
   catch
    return null
   @buffer = new Buffer CHUNK
   @bufferPos = 0
   @bufferEnd = 0
   @fdPos = @cursor
   @nextCursor = @cursor
   @topValue = null

  if @topValue?
   return @topValue

  while true
   i = @bufferPos
   while i < @bufferEnd and @buffer[i] isnt NEW_LINE
    @nextCursor++
    i++
   str += @buffer.toString 'utf8', @bufferPos, i
   if i < @bufferEnd
    i++
    @nextCursor++
    @bufferPos = i
    @topValue = JSON.parse str
    return @topValue
   else
    @bufferEnd = FS.readSync @fd, @buffer, 0, CHUNK, @fdPos
    @fdPos += @bufferEnd
    @bufferPos = 0

    if @bufferEnd is 0
     return null

 moveSync: (target) ->
  if target?
   @sync.sync()
   if target.syncFile isnt @syncFile
    target.sync.sync()
   return on
  return off

 # server events

 setupServer: ->
  IO.Client.on 'top', (data, options, res) =>
   @on.top (data) =>
    res.success data

  IO.Client.on 'pop', (data, options, res) =>
   @on.pop (result) =>
    if result is on
     @sync.sync()
    res.success result

  IO.Client.on 'push', (json, options, res) =>
   @on.push json, (result) =>
    if result is on
     @sync.sync()
    res.success result

  IO.Client.on 'move', (targetId, options, res) =>
   @on.move targetId, (target) =>
    res.success @moveSync target

 #events

 @listen 'top', (callback) ->
  @readTop()
  callback @topValue

 @listen 'pop', (callback) ->
  res = @moveCursor()
  callback res

 @listen 'pushed', ->
  for cb in @onPushed
   cb?()

 @listen 'push', (json, callback) ->
  if not json?
   callback off
  else
   str = JSON.stringify json
   @qFile.append "#{str}\n"
   callback on
   setTimeout @on.pushed, 0

 @listen "move", (targetId, callback) ->
  target = queues[targetId]
  if target?
   @on.top (top) =>
    if not top?
     callback null
    else
     target.on.push top, (result) =>
      if not result
       callback null
      else
       @on.pop =>
        callback target
  else
   callback null

 # API

 top: (callback) ->
  if not @started
   return off

  if @client is on
   IO.Server.send 'top', null, (data) =>
    callback data
  else
   @on.top callback

 pop: (callback) ->
  if not @started
   return off

  if @client is on
   IO.Server.send 'pop', null,  (res) =>
    callback res
  else
   @on.pop (res) =>
    if res is on
     @sync.sync()
    callback res

 push: (json, callback) ->
  if not @started
   return off

  if @client is on
   IO.Server.send 'push', json,  (result) =>
    callback result
  else
   @on.push json, (result) =>
    if result is on
     @sync.sync()
    callback result

 move: (targetId, callback) ->
  if not @started
   return off
  if @client is on
   IO.Server.send 'move', targetId, (result) =>
    callback result
  else
   @on.move targetId, (target) =>
    callback @moveSync target

 subscribe: (callback) ->
  if @client is on
   return off
  @onPushed.push callback
  return on

 onStarted: (cb) ->
  if @started is on
   cb?()
  else
   @startCB.push cb

module.exports = MessageQueue
