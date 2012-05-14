#!/usr/bin/env /usr/bin/python
import sys
sys.path.append('/home/piers/git/public/python-hdfsio')
import hdfsio

f = hdfsio.file('/user/hduser/hdfsiotest')
print "exists: ", f.name, f.exists()
print "errors: ", f.error
print "mkdir: ", f.name, f.mkdir()
print "errors: ", f.error
print "exists: ", f.name, f.exists()
print "errors: ", f.error

f = hdfsio.file('/user/hduser/hdfsiotest/blah.txt')
print "exists: ", f.name, f.exists()
print "errors: ", f.error
print "rmr: ", f.name, f.rmr()
print "errors: ", f.error
print "write: ", f.name, f.write("the\nquick\nbrow fox\njumped over\n")
print f.get().split("\n")
print len(f.get())

# get it again with a file handle returned
h = f.get(True)
for i in h:
    print "line: ", i.strip()

f = hdfsio.file('/user/hduser/hdfsiotest')
print "ls: ", f.name, f.ls()
print "errors: ", f.error
print "rmr: ", f.name, f.rmr()
print "errors: ", f.error
print "exists: ", f.name, f.exists()
print "errors: ", f.error

sys.exit(0)

