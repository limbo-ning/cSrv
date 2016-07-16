#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json,sys,traceback,threading,os,ConfigParser,subprocess
sys.path.append("./BaseMQ/")
import BaseMQUtil
import ScheduleMain
import traceback

PWD = sys.path[0]
if os.path.isfile(PWD):
	PWD = os.path.dirname(PWD)

threadLocal = threading.local()

def spawnProcess(options):
	if options.has_key('path') and len(options.get('path')) > 0:
		os.chdir(options.get('path'))
	process = subprocess.Popen(cmd.split(' '))
	os.chdir(PWD)
	
	return process

def spawnThread(module,options,LOG):
	thread = taskThread(module, options,LOG)
	return thread


class ScheduleTaskExecutor():
	def __init__(self, taskDict):
		self._taskDict = taskDict
		self._processList = []
		self._threadList = []

		configFile = taskDict.values()[0].get('config_file')

		self.LOG = BaseMQUtil.getLogger(configFile, logFolder=ScheduleMain.LOG_FOLDER+'/'+configFile, logFile='executor')

	def start(self):
		taskNames = self._taskDict.keys()

		for task in taskNames:
			taskOption = self._taskDict.get(task)
			if taskOption.get('process_or_thread') == 'process':
				process = spawnProcess(taskOption)
				self._processList.append(process)
				self.LOG.debug('process %s started' % process.pid)

			elif taskOption.get('process_or_thread') == 'thread':
				thread = spawnThread(task, taskOption, self.LOG)
				self._threadList.append(thread)
				thread.setDaemon(True)
				thread.setName(task)
				thread.start()
				self.LOG.debug('thread %s started' % thread.name)


		for process in self._processList:
			process.wait()
			self.LOG.debug('process %s ended' % process.pid)
			if process.returncode != 0:
				self.LOG.error('process %s return error' % process.pid)

		for thread in self._threadList:
			thread.join()
			self.LOG.debug('thread %s ended' % thread.name)

class taskThread(threading.Thread):
	def __init__(self, module, options, LOG):
		super(taskThread, self).__init__()
		self._module = module
		if options.has_key('path') and len(options.get('path')) > 0:
			self._path = options.path
		else:
			self._path = ''
		self._cmd = options.get('cmd')
		self.LOG = LOG

	def run(self):
		try:
			if len(self._path) > 0:
				sys.path.append(self._path)

			moduleName = self._module[:len(self._module)-3]

			exec('import %s' % moduleName)
			exec('%s.%s' % (moduleName, self._cmd))

		except Exception as e:
			self.LOG.error(traceback.format_exc())


def execute(configFilePath):
	config = ScheduleMain.readConfig(configFilePath)

	sections = config.sections()

	taskDict = {}
	for section in sections:
		if config.has_option(section, 'run_by_executor') and config.getboolean(section, 'run_by_executor'):
			taskDict[section] = dict(config.items(section))

	executor = ScheduleTaskExecutor(taskDict)
	executor.start()



if __name__ == "__main__":
	if len(sys.argv) != 2:
		raise Exception('Wrong input. arg: configFilePath')

	execute(sys.argv[1])


