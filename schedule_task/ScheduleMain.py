#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import ConfigParser,threading
import os
import ScheduleListener

CONFIG_FOLDER = 'SCHEDULE_CONF'
CONFIG_SUFFIX = '.conf'

LOG_FOLDER = 'SCHEDULE_LOG'

SCHEDULE_ID_VALIDATE = {}


def validate(config):
	defaults = dict(config.defaults())

	configFile = defaults.get('config_file')
	mode = defaults.get('mode')
	scheduleId = defaults.get('schedule_id')
	module = defaults.get('module')

	if SCHEDULE_ID_VALIDATE.has_key(scheduleId):
		raise Exception('duplicate scheduleId with config [%s] and [%s]' % (configFile, SCHEDULE_ID_VALIDATE.get(scheduleId)))

	SCHEDULE_ID_VALIDATE[scheduleId] = configFile


	sections = config.sections()

	if module not in sections:
		raise Exception('module section not found in config [%s]' % configFile)

	for section in sections:
		mode = config.get(section, 'process_or_thread')
		cmd = config.get(section, 'cmd')

		if mode == 'thread':
			if not section.endswith('.py'):
				raise Exception('thread mode can only apply to python script config [%s] section [%s]' % (configFile, section))
		elif mode == 'process':
			pass
		else:
			raise Exception('process_or_thread incorrect config [%s] section [%s]' % (configFile, section))

		executablePath = ''
		if config.has_option(section, 'path'):
			path = config.get(section, 'path')
			if len(path) != 0:
				if path.endswith('/'):
					executablePath += path
				else:
					executablePath += path + '/'
		executablePath += section

		if not os.path.exists(executablePath):
			raise Exception('executable not found. path [%s] config [%s]' % (executablePath, configFile))

class workerThread(threading.Thread):
	def __init__(self, config):
		super(workerThread, self).__init__()
		self.listener = ScheduleListener.ScheduleListener(config)

	def run(self):
		self.listener.start()


def spawnListenerThread(config):
	thread = workerThread(config)
	thread.setDaemon(True)
	thread.start()
	return thread


def readConfig(configFileName):
	config = ConfigParser.ConfigParser()
	config.read(CONFIG_FOLDER+'/'+configFileName)

	config.set('DEFAULT', 'config_file', configFileName)

	validate(config)

	return config


if __name__ == '__main__':
	if not os.path.exists(CONFIG_FOLDER) or not os.path.isdir(CONFIG_FOLDER):
		raise Exception('%s folder not found' % CONFIG_FOLDER)

	configFiles = os.listdir(CONFIG_FOLDER)

	configList = []

	for configFile in configFiles:
		if not configFile.endswith(CONFIG_SUFFIX):
			continue

		configList.append(readConfig(configFile))

	threadList = []

	for config in configList:
		threadList.append(spawnListenerThread(config))

	for thread in threadList:
		thread.join()
		raise Exception('listener joined')

	


