#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import json,sys,os,subprocess,signal
sys.path.append("BaseMQ/")
import BaseMQUtil
import traceback

LOG = BaseMQUtil.getLogger(__name__, logFile='scheduleExecutor')

PWD = sys.path[0]
if os.path.isfile(PWD):
	PWD = os.path.dirname(PWD)


def runTaskList(scriptBaseFolder, taskList):

	LOG.debug('executor run. baseFolder:%s taskList:%s' % (scriptBaseFolder, repr(taskList)))

	if not os.path.exists(PWD + '/' + scriptBaseFolder):
		raise Exception('script base folder %s not exists. skip' % PWD+'/'+scriptBaseFolder)

	subProcessList = []

	try:
		for task in taskList:
			subProcessList.append(runTask(scriptBaseFolder, task))

		for subProcess in subProcessList:
			subProcess.wait()
			LOG.debug('task done. pid: %d' % subProcess.pid)
			if subProcess.returncode != 0:
				raise Exception('task[%d] returned error. abort all.' % subProcess.pid)

	except Exception as e:
		LOG.error(traceback.format_exc())
		for subProcess in subProcessList:
			try:
				subProcess.send_signal(signal.SIGTERM)
			except:
				LOG.warn('kill failed. pid:' % subProcess.pid)

		raise e

		
def runTask(scriptBaseFolder, taskInfo):
	path = PWD + '/' + scriptBaseFolder

	if taskInfo.has_key('path'):
		path = taskInfo.get('path').encode('utf-8')

		if path[0] != '/':
			path += '/' + path

		if not os.paths.exists(path):
			raise Exception('task path not exists: %s' % path)

	if not taskInfo.has_key('cmd'):
		raise Exception('task must have cmd. taskInfo:%s' % (BaseMQUtil.toJsonStr(taskInfo)))

	os.chdir(path)

	subProcess = subprocess.Popen(taskInfo.get('cmd').split(' '))
	LOG.debug('task run. pid: %d' % subProcess.pid)

	os.chdir(PWD)

	return subProcess


