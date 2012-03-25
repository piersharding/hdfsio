
"""
Python utils for importing IDE CSV format files
"""

import sys
if sys.version < '2.6':
    print 'Wrong Python Version (must be >=2.6) !!!'
    sys.exit(1)

# load the native extensions
import subprocess as sub
import re
import logging

HADOOP = 'hadoop'

class HDFSIOException(Exception):
    def __init__(self, value):
        self.value = value
        logging.info("HDFSIOException: " + str(self))

    def __str__(self):
        return repr(self.value)


class file(object):
    """
    Basic file object to access IO functions of HDFS
    """
    def __init__(self, name):
        self.error = None
        self.name = name

    """
    Does the file exist in HDFS
    """
    def exists(self):
        output = False
        errors = False
        self.error = None
        p = False
        f = []
        try:
            p = sub.Popen([HADOOP, 'fs', '-ls', self.name], stdout=sub.PIPE, stderr=sub.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on ls for: ' + self.name)
        
        if len(errors) > 0 or p.wait() != 0:
            self.error = errors + ' rc: ' + str(p.wait())
            return False
       
        ls = output.split("\n")
        if len(ls) <= 1:
            return False

        # remove the title line of output
        ls.pop(0)
        if len(ls) == 1:
            l = ls[0].split(" ").pop()
            if l == self.name:
                return True
        for l in ls:
            f = l.split(" ")
            i = f.pop()
            if re.match('^'+self.name, i):
                return True

        return False

    """
    Get the contents of the file
    """
    def get(self):
        output = False
        errors = False
        p = False
        f = []
        try:
            p = sub.Popen([HADOOP, 'fs', '-get', self.name, '-'], stdout=sub.PIPE, stderr=sub.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on get for: ' + self.name)
        
        if len(errors) > 0 or p.wait() != 0:
            raise HDFSIOException('IO failed on get for: ' + self.name + ' - ' + errors + ' rc: ' + str(p.wait()))
        
        return output


    """
    Remove the file or directory from HDFS
    """
    def rmr(self):
        output = False
        errors = False
        p = False
        f = []
        if not self.exists():
            return True

        try:
            p = sub.Popen([HADOOP, 'fs', '-rmr', self.name], stdout=sub.PIPE, stderr=sub.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on rmr for: ' + self.name)
        
        if len(errors) > 0 or p.wait() != 0:
            raise HDFSIOException('IO failed on rmr for: ' + self.name + ' - ' + errors + ' rc: ' + str(p.wait()))
        
        return True


    """
    Write to the file - will error if already exists
    """
    def write(self, data):
        output = False
        errors = False
        p = False
        f = []
        if not type(data) == list:
            data = [str(data)]
        try:
            p = sub.Popen([HADOOP, 'fs', '-copyFromLocal', '-', self.name], stdin=sub.PIPE, stdout=sub.PIPE, stderr=sub.PIPE)
            output, errors = p.communicate(input="\n".join(data))
        except OSError:
            raise HDFSIOException('OSError on write for: ' + self.name)
        
        if len(errors) > 0 or p.wait() != 0:
            raise HDFSIOException('IO failed on write for: ' + self.name + ' - ' + errors + ' rc: ' + str(p.wait()))
        
        return True

