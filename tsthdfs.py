#!/usr/bin/env /usr/bin/python
import sys
sys.path.append('/home/piers/git/public/python-hdfsio')
import hdfsio

f = hdfsio.file('/user/hduser/gutenberg/19486-0.txt')
#f = hdfsio.file('/user/hduser/gutenberg/')

print "exists: ", f.name, f.exists()
print len(f.get())

f = hdfsio.file('/user/hduser/gutenberg/blah.txt')
print "exists: ", f.name, f.exists()
print "rmr: ", f.name, f.rmr()
print "write: ", f.name, f.write("the\nquick\nbrow fox\njumped over\n")
print f.get().split("\n")
print len(f.get())
print "rmr: ", f.name, f.rmr()

sys.exit(0)

