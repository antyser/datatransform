import beanstalkc
from pprint import pprint
PRODUCE_CUBE = 'seeds'
BEANSTALK_HOST = '172.31.10.154'
BEANSTALK_PORT = 11300
beanstalk = beanstalkc.Connection(host=BEANSTALK_HOST, port=BEANSTALK_PORT)
pprint(beanstalk.tubes())
beanstalk.watch(PRODUCE_CUBE)
while (True):
    job = beanstalk.reserve()
    print job.body
    job.delete()
