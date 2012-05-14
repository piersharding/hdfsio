
"""
Python util for accessing Hadoop HDFS
"""

import sys
if sys.version < '2.6':
    print 'Wrong Python Version (must be >=2.6) !!!'
    sys.exit(1)

# load the native extensions
import subprocess
import tempfile
import re
import logging

# the hadoop executable path
HADOOP = 'hadoop'

class HDFSIOException(Exception):
    """
    Exception that hdfsio.file will throw on any IO problem
    """
    def __init__(self, value):
        self.value = value
        logging.info("HDFSIOException: " + str(self))

    def __str__(self):
        return repr(self.value)


class file(object):
    """
    hdfsio.file

    main class
    define an HDFS file/directory object and then do something with it

    import hdfsio
    f = hdfsio.file('thingy.txt')
    if f.exists():
        do something

    data = f.get()

    """

    def __init__(self, name):
        """
        Basic file object to access IO functions of HDFS
        """
        self.error = None
        self.name = name

    def exists(self):
        """
        Does the file exist in HDFS
        """
        output = False
        errors = False
        self.error = None
        p = False
        f = []
        try:
            p = subprocess.Popen([HADOOP, 'fs', '-ls', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on ls for: ' + self.name)
        
        if (len(errors) > 0 and errors.strip() != 'Warning: $HADOOP_HOME is deprecated.') or p.wait() != 0:
            self.error = errors + ' rc: ' + str(p.wait())
            return False

        return True


    def ls(self):
        """
        List the file or directory
        """
        output = False
        errors = False
        self.error = None
        p = False
        f = []
        try:
            p = subprocess.Popen([HADOOP, 'fs', '-ls', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on ls for: ' + self.name)
        
        if (len(errors) > 0 and errors.strip() != 'Warning: $HADOOP_HOME is deprecated.') or p.wait() != 0:
            self.error = errors + ' rc: ' + str(p.wait())
            return False

        ls = output.strip().split("\n")
        if len(ls) <= 1:
            return False

        # remove the title line of output
        ls.pop(0)
        data = []
        for l in ls:
            data.append(l.split(" ").pop())

        return data


    def get(self, handle=False):
        """
        Get the contents of the file
        parameters:
          * handle - True/False
            pass back an open file handle to the HDFS object contents
        """
        output = False
        errors = False
        p = False
        tmp = False
        try:
            if handle:
                tmp = tempfile.mktemp()
                p = subprocess.Popen([HADOOP, 'fs', '-get', self.name, tmp], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                p = subprocess.Popen([HADOOP, 'fs', '-get', self.name, '-'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on get for: ' + self.name)
        
        if (len(errors) > 0 and errors.strip() != 'Warning: $HADOOP_HOME is deprecated.') or p.wait() != 0:
            raise HDFSIOException('IO failed on get for: ' + self.name + ' - ' + errors + ' rc: ' + str(p.wait()))
       
        if handle:
            return open(tmp)
        else:
            return output


    def rmr(self):
        """
        Remove the file or directory from HDFS
        """
        output = False
        errors = False
        p = False
        f = []
        if not self.exists():
            return True

        try:
            p = subprocess.Popen([HADOOP, 'fs', '-rmr', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on rmr for: ' + self.name)
        
        if (len(errors) > 0 and errors.strip() != 'Warning: $HADOOP_HOME is deprecated.') or p.wait() != 0:
            raise HDFSIOException('IO failed on rmr for: ' + self.name + ' - ' + errors + ' rc: ' + str(p.wait()))
        
        return True

    def mkdir(self):
        """
        Create a directory
        """
        output = False
        errors = False
        p = False
        f = []
        if self.exists():
            return False

        try:
            p = subprocess.Popen([HADOOP, 'fs', '-mkdir', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, errors = p.communicate()
        except OSError:
            raise HDFSIOException('OSError on mkdir for: ' + self.name)
        
        if (len(errors) > 0 and errors.strip() != 'Warning: $HADOOP_HOME is deprecated.') or p.wait() != 0:
            raise HDFSIOException('IO failed on mkdir for: ' + self.name + ' - ' + errors + ' rc: ' + str(p.wait()))
        
        return True


    def write(self, data):
        """
        Write to the file - will error if already exists
        """
        output = False
        errors = False
        p = False
        f = []
        if not type(data) == list:
            data = [str(data)]
        try:
            p = subprocess.Popen([HADOOP, 'fs', '-copyFromLocal', '-', self.name], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, errors = p.communicate(input="\n".join(data))
        except OSError:
            raise HDFSIOException('OSError on write for: ' + self.name)
        
        if (len(errors) > 0 and errors.strip() != 'Warning: $HADOOP_HOME is deprecated.') or p.wait() != 0:
            raise HDFSIOException('IO failed on write for: ' + self.name + ' - ' + errors + ' rc: ' + str(p.wait()))
        
        return True

