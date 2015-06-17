import beanstalkc

PRODUCE_CUBE = 'seeds'
BEANSTALK_HOST = '172.31.10.154'
BEANSTALK_PORT = 11300
beanstalk = beanstalkc.Connection(host=BEANSTALK_HOST, port=BEANSTALK_PORT)
beanstalk.use(PRODUCE_CUBE)
while (True):
    job = beanstalk.reserve()
    job.delete()