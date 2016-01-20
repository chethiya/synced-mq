Base = require './weya/base'
SyncAppend = require './sync-append/sync-append'
IO = require './IO/io'
Session = require './IO/session'
http = require 'http'
PATH = require 'path'
FS = require 'fs'

groups = {}
syncs = {}

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
   files["#{@id}.pos"] = "#{@filePath}.pos"
   @sync.start files
   @sync.syncStop()
   @qFile = @sync.getFile @id
   @posFile = @sync.getFile "#{@id}.pos"

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

 setupServer: ->
  IO.Client.on 'top', (data, options, res) =>
   @on.top (data) =>
    res.success data

 @listen 'top', (callback) ->
  callback @cnt++

 top: (callback) ->
  if not @started
   return off

  if @client is on
   IO.Server.send 'top', null, (data) =>
    callback data
  else
   @on.top callback

 onStarted: (cb) ->
  if @started is on
   cb?()
  else
   @startCB.push cb

module.exports = MessageQueue
