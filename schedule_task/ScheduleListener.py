#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json,sys,traceback,threading,os,ConfigParser,subprocess
sys.path.append("BaseMQ/")
import BaseMQUtil
import ScheduleMQ, ScheduleMain

PWD = sys.path[0]
if os.path.isfile(PWD):
	PWD = os.path.dirname(PWD)

def spawnProcess(options):
	if options.has_key('path') and len(options.get('path')) > 0:
		os.chdir(options.get('path'))
	subProcess = subprocess.Popen(options.get('cmd').split(' '))
	os.chdir(PWD)

	return subProcess

def spawnThread(module,options):
	thread = executorThread(module, options)
	return thread

class ScheduleListener():
	def __init__(self, config):
		self._configFile = config.get('DEFAULT', 'config_file')
		self._queue = config.get('DEFAULT', 'queue')

		self._scheduleType = config.getint('DEFAULT', 'schedule_type')
		self._scheduleId = config.getint('DEFAULT', 'schedule_id')

		self._module = config.get('DEFAULT', 'module')
		self._module_options = dict(config.items(self._module))

		self.LOG = BaseMQUtil.getLogger(self._configFile, logFolder=ScheduleMain.LOG_FOLDER+'/'+self._configFile, logFile='listener')

		self.mq = ScheduleMQ.ScheduleMQHandler(logger = self.LOG)

	def start(self):
		self.mq.listen_schedule_mq(self.buildProcessFunction(), queue = self._queue)


	def buildProcessFunction(self):
		def processFunction(msg, properties):

			self.LOG.debug('received msg:%s properties:%s', BaseMQUtil.toJsonStr(msg), BaseMQUtil.toJsonStr(properties))
			if properties.get('type') != self._scheduleType:
				self.LOG.debug('scheduleType not match. skip')
				return
			if properties.get('subType') != self._scheduleId and properties.get('subType') != ScheduleMQ.PROPERTIES_SUB_TYPE_SCHEDULE_ALL:
				self.LOG.debug('scheduleId not match. skip')
				return

			if self._module_options.get('process_or_thread') == 'process':
				subProcess = spawnProcess(self._module_options)
				self.LOG.debug('process spawned. module:%s options:%s' % (self._module, BaseMQUtil.toJsonStr(self._module_options)))
				subProcess.wait()
				self.LOG.debug('process completed')
				if subProcess.returncode != 0:
					raise Exception('task %s failed' % self._module)


			elif self._module_options.get('process_or_thread') == 'thread':
				thread = executorThread(self._module, self._module_options, self.LOG)
				thread.setDaemon(True)
				errorFlag = thread.begin()
				self.LOG.debug('thread spawned. module:%s options:%s' % (self._module, BaseMQUtil.toJsonStr(self._module_options)))
				thread.join()
				self.LOG.debug('thread joined')
				if errorFlag.isSet():
					raise Exception('task %s failed' % self._module)


		return processFunction
	
		
class executorThread(threading.Thread):
	def __init__(self, module, options, LOG):
		super(executorThread, self).__init__()
		self._module = module
		if options.has_key('path') and len(options.get('path')) > 0:
			self._path = options.path
		else:
			self._path = ''
		self._cmd = options.get('cmd')
		self._errorFlag = threading.Event()
		self.LOG = LOG

	def begin(self):
		self._errorFlag.clear()
		self.start()
		return self._errorFlag

	def run(self):
		try:
			if len(self._path) > 0:
				sys.path.append(self._path)

			moduleName = self._module[:len(self._module)-3]

			exec('import %s' % moduleName)
			exec('%s.%s' % (moduleName, self._cmd))

		except Exception as e:
			self.LOG.error(traceback.format_exc())
			self._errorFlag.set()

