__author__ = 'junliu'
import json,time
import sys
label = sys.argv[1]
stage = sys.argv[2]

with open('status.log') as file:
    count = 0
    success = 0
    fail = 0
    current_ts = long(time.time())
    threshold = current_ts - 24*3600
    for line in file:
        obj = json.loads(line)
        if obj['label'] != label or obj['stage'] != stage or obj['ts'] < threshold:
            continue

        count += 1
        if obj['is_succeed']:
            success += 1
        else:
            fail += 1
    print "total : " + str(count) + " success: " + str(success) + " fail: " + str(fail)
