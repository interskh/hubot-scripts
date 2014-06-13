# Description:
#
#   Viewing luigi stats
#
# Commands:
#   hubot luigi stats - Show overall stats
#   hubot luigi show <query> - List RUNNING/PENDING/DONE tasks
#   hubot luigi search <query> - Search task by task id
#   
# Configuration:
#   HUBOT_LUIGI_ENDPOINT - luigi api endpoint, like 'http://luigi.com/api/'
#
# URLS:
#   https://github.com/spotify/luigi/
#
# Author:
#   interskh


luigiApiEndpoint = process.env.HUBOT_LUIGI_ENDPOINT

module.exports = (robot) ->
  robot.respond /luigi statu?s(\s*)$/i, (msg) ->
    callLuigiTaskList msg, "RUNNING", (res) ->
      running = Object.keys(res).length
      callLuigiTaskList msg, "PENDING", (res) ->
        pending = Object.keys(res).length
        callLuigiTaskList msg, "FAILED", (res) ->
          failed = Object.keys(res).length
          msg.send running + " jobs running, " + pending + " jobs pending, " + failed + " jobs failed"

  robot.respond /luigi show( all)? (.*)(\s*)/i, (msg) ->
    status = msg.match[2].toUpperCase()
    callLuigiTaskList msg, status, (res) ->
      results = []
      for t in sortTask(res)
        results.push(formatTask(t[0], t[1]))
      if results.length > 0
        msg.send results.join("\n")

  robot.respond /luigi search (.*)(\s*)/i, (msg) ->
    callLuigiTaskSearch msg, msg.match[1], (res) ->
      results = []
      for status, d of res
        for t in sortTask(d)
          results.push(status + " " + formatTask(t[0], t[1]))
      if results.length > 0
        msg.send results.join("\n")

callLuigiTaskList = (msg, jobType, cb) ->
  msg.http(luigiApiEndpoint + "task_list")
    .query(data: JSON.stringify({status: jobType, upstream_status: ""}))
    .get() (err, res, body) ->
      try
        ret = JSON.parse body
        cb ret.response
      catch error
        console.log body
        console.log error
        cb {}

callLuigiTaskSearch = (msg, str, cb) ->
  msg.http(luigiApiEndpoint + "task_search")
    .query(data: JSON.stringify({task_str: str}))
    .get() (err, res, body) ->
      try
        ret = JSON.parse body
        cb ret.response
      catch error
        console.log body
        console.log error
        cb {}

sortTask = (taskDict) ->
  # tasks in taskDict should be in the same status
  sortable = []
  for task_id, task of taskDict
    status = task.status
    sortable.push([task_id, task])
  if status == 'RUNNING'
    sortable.sort (a,b) -> a[1].time_running - b[1].time_running
  else
    sortable.sort (a,b) -> a[1].start_time - b[1].start_time

formatTask = (task_id, task) ->
  if task.status == 'RUNNING'
    formatTime(task.time_running) + " " + task_id
  else
    formatTime(task.start_time) + " " + task_id

formatTime = (ts) ->
  new Date(Math.floor(ts*1000)).toLocaleString()
