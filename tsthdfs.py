#!/usr/bin/env /usr/bin/python
import sys
sys.path.append('/home/piers/git/public/python-hdfsio')
import hdfsio

f = hdfsio.file('/user/hduser/hdfsiotest')
print "exists: ", f.name, f.exists()
print "mkdir: ", f.name, f.mkdir()
print "exists: ", f.name, f.exists()

f = hdfsio.file('/user/hduser/hdfsiotest/blah.txt')
print "exists: ", f.name, f.exists()
print "rmr: ", f.name, f.rmr()
print "write: ", f.name, f.write("the\nquick\nbrow fox\njumped over\n")
print f.get().split("\n")
print len(f.get())

# get it again with a file handle returned
h = f.get(True)
for i in h:
    print "line: ", i.strip()

f = hdfsio.file('/user/hduser/hdfsiotest')
print "rmr: ", f.name, f.rmr()
print "exists: ", f.name, f.exists()

sys.exit(0)

