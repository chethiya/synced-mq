Base = require './weya/base'
SyncAppend = require './sync-append/sync-append'
IO = require './IO/io'
Session = require './IO/session'
http = require 'http'
PATH = require 'path'
FS = require 'fs'

groups = {}
syncs = {}

CHUNK = 1<<17 # 128KB
SPACE = ' '.charCodeAt 0
NEW_LINE = '\n'.charCodeAt 0

class MessageQueue extends Base
 @extend()

 # server - port?, sync, filePath, sync (syncFilePath)
 # client - host, port

 @initialize (opt) ->
  @startCB = []
  @started = off
  @cnt = 0

  if not opt?
   throw new Error "No options passed in for the message queue"

  @client = opt.client
  @client ?= off

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
    @sync = syncs[@syncFile] = new SyncAppend @syncFile

   files = {}
   files[@id] = @filePath
   @cursorPath = "#{@filePath}.cursor"
   files["#{@id}.cursor"] = @cursorPath
   @sync.syncStop()
   @sync.start files
   @qFile = @sync.getFile @id
   @cursorFile = @sync.getFile "#{@id}.cursor"

   @cursor = @readCursor()
   @top = null
   @buffer = null

   if @port?
    server = new IO.ports.NodeHttpServerPort port: @port, http
    server.wrap Session.MultiSession
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
   port.wrap Session.SingleSession
   IO.addPort 'Server', port
   IO.Server.newSession (session) =>
    @started = on
    for cb in @startCB
     cb?()

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
  @cursor = @nextCursor
  @top = null
  @cursorFile.append "#{@cursor} "

 readTop: ->
  str = ''
  if not @buffer?
   @buffer = new Buffer CHUNK
   @bufferPos = 0
   @bufferEnd = 0
   @fd = FS.openSync @filePath, 'r'
   @fdPos = @cursor
   @nextCursor = @cursor
   @top = null

  if @top?
   return @top

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
    @top = JSON.parse str
    return @top
   else
    @bufferEnd = FS.readSync @fd, @buffer, 0, CHUNK, @fdPos
    @fdPos += @bufferEnd
    @bufferPos = 0

    if @bufferEnd is 0
     return null

 # server events

 setupServer: ->
  IO.Client.on 'top', (data, options, res) =>
   @on.top (data) =>
    res.success data

  IO.Client.on 'pop', (data, options, res) =>
   @on.pop =>
    @sync.sync()
    res.success()

  IO.Client.on 'push', (json, options, res) =>
   @on.push json, =>
    @sync.sync()
    res.success()

 #events

 @listen 'top', (callback) ->
  @readTop()
  callback @top

 @listen 'pop', (callback) ->
  @moveCursor()
  callback()

 @listen 'push', (json, callback) ->
  str = JSON.stringify json
  @qFile.append "#{str}\n"
  callback()

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
   IO.Server.send 'pop', null,  =>
    callback()
  else
   @on.pop =>
    @sync.sync()
    callback()

 push: (json, callback) ->
  if not @started
   return off

  if @client is on
   IO.Server.send 'push', json,  =>
    callback()
  else
   @on.push =>
    @sync.sync()
    callback()


 onStarted: (cb) ->
  if @started is on
   cb?()
  else
   @startCB.push cb

module.exports = MessageQueue
