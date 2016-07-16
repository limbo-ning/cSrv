#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import json,sys
sys.path.append("./BaseMQ/")
import BaseMQConfig, BaseMQUtil, BaseMQHandler
import traceback

#
# Rabbit MQ Config
#
RABBITMQ_USER_NAME 		=	"brain"
RABBITMQ_PASSWORD		=	"123456"

RABBITMQ_HOST_NAME		=	"rdpi05"
RABBITMQ_HOST_PORT		=	5672

DEFAULT_FANOUT_VHOST 	=	"/schedule"
DEFAULT_FANOUT_EXCHANGE	=	"scheduleExchange"

MAX_SCHEDULE_TASK = 50

PROPERTIES_TYPE_SCHEDULE_INDEX		=	20#计算指数
PROPERTIES_TYPE_SCHEDULE_RULE		=	21#计算规则
PROPERTIES_TYPE_SCHEDULE_DUMP		=	22#定时dump
PROPERTIES_TYPE_SCHEDULE_EVENT		=	23#触发事件

PROPERTIES_SUB_TYPE_SCHEDULE_ALL	=	-1#全部脚本


def processFunction(msg, properties):
	raise NotImplementedError

class ScheduleMQHandler(BaseMQHandler.BaseMQHandler):
	def __init__(
					self, 
					host=RABBITMQ_HOST_NAME, 
					port=RABBITMQ_HOST_PORT, 
					vhost=DEFAULT_FANOUT_VHOST, 
					user=RABBITMQ_USER_NAME, 
					password=RABBITMQ_PASSWORD,
					logger=None
				):

		if logger != None:
			self.LOG = logger
		else:
			self.LOG = BaseMQUtil.getLogger(__name__)

		BaseMQHandler.BaseMQHandler.__init__(self, host,port,vhost,user,password,logger=self.LOG)

		self.connect(exchange=DEFAULT_FANOUT_EXCHANGE)

	def buildAction(self, processFunction):
		# Message handler when consuming message
		def action(channel, method_frame, header_frame, body):
			content = json.loads(body)
			self.LOG.debug('received schedule mq. body:%s' % body)

			properties = content.get('properties')
			if properties.get("type") not in [PROPERTIES_TYPE_SCHEDULE_INDEX, PROPERTIES_TYPE_SCHEDULE_RULE, PROPERTIES_TYPE_SCHEDULE_DUMP, PROPERTIES_TYPE_SCHEDULE_EVENT]:
				self.LOG.warn('invalid schedule type. skip')
				return
			
			msg = content.get('msg')

			processFunction(msg, properties)

		return action

	def send_schedule_mq(self, msg, properties):
		self.publish_broadcast_mq(properties, msg=msg)

	#this method will block
	def listen_schedule_mq(self, processFunction, queue='',autoAck=False):
		self.set_broadcast_consumer(action=self.buildAction(processFunction),queue=queue,autoAck=autoAck)
		self.start_consuming()



