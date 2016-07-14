#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json,sys,traceback,threading,os
sys.path.append("BaseMQ/")
import BaseMQUtil
import ScheduleMQ
import ScheduleTaskExecutor

CLUSTER_ID = 1

MAX_TASK_PER_EXECUTOR = 1

SCHEDULE_FOLDER = {
	ScheduleMQ.PROPERTIES_TYPE_SCHEDULE_INDEX	:	'index',
	ScheduleMQ.PROPERTIES_TYPE_SCHEDULE_RULE	:	'rule',
	ScheduleMQ.PROPERTIES_TYPE_SCHEDULE_DUMP	:	'dump',
	ScheduleMQ.PROPERTIES_TYPE_SCHEDULE_EVENT	:	'event'
}

LOG = BaseMQUtil.getLogger(__name__, logFile='scheduleListener')

def processFunction(msg, properties):
	taskClusterId = properties.get('subType')

	if taskClusterId != CLUSTER_ID and taskClusterId != -1:
		LOG.debug('not for me. skip')
		return

	scriptBaseFolder = SCHEDULE_FOLDER.get(properties.get('type'))

	if not os.path.exists(scriptBaseFolder):
		LOG.debug('script folder of type %s not exists. skip' % scriptBaseFolder)
		return

	taskList = msg.get('taskList')
	taskCount = len(taskList)

	if taskCount == 0:
		LOG.warn('taskList empty. skip')
		return

	workThreads = []
	errorFlags = []

	if taskCount < MAX_TASK_PER_EXECUTOR:
		thread = executorThread(scriptBaseFolder, taskList)
		thread.setName('ExecutorThread-%d' % len(workThreads))
		workThreads.append(thread)
		errorFlags.append(thread.begin())
	else:
		for i in range(0, taskCount, MAX_TASK_PER_EXECUTOR):
			begin = i
			if i >= (taskCount - MAX_TASK_PER_EXECUTOR):
				end = taskCount
			else:
				end = i + MAX_TASK_PER_EXECUTOR
			thread = executorThread(scriptBaseFolder, taskList[begin : end])
			thread.setName('ExecutorThread-%d' % len(workThreads))
			workThreads.append(thread)
			errorFlags.append(thread.begin())
	
		
	for thread in workThreads:
		thread.join()
		LOG.debug('%s joined' % thread.name)


	for errorFlag in errorFlags:
		if errorFlag.isSet():
			raise Exception('task failed')

	
		
class executorThread(threading.Thread):
	def __init__(self, scriptBaseFolder, taskList):
		super(executorThread, self).__init__()
		self.scriptBaseFolder=scriptBaseFolder
		self.taskList = taskList
		self.errorFlag = threading.Event()

	def begin(self):
		self.errorFlag.clear()
		self.start()
		return self.errorFlag

	def run(self):
		try:
			ScheduleTaskExecutor.runTaskList(self.scriptBaseFolder, self.taskList)
		except Exception as e:
			LOG.error('%s error: %s' % (self.name, str(e)))
			self.errorFlag.set()


if __name__ == '__main__':
	scheduleMQ = ScheduleMQ.ScheduleMQHandler(logger=LOG)
	scheduleMQ.listen_schedule_mq(processFunction)


