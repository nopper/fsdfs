import time
import threading

class Replicator(threading.Thread):
    '''
	This is the Replicator class.
	It manages files on every node.
	
	It takes a Filesystem instance in argument which is saved in self.fs.
    '''

    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs
        self.filedb = None
        self.stopnow = False

    def shutdown(self):
        '''
        Shutdown the server by setting the variable self.stopnow to True.

        It breaks the while which runs the Replicator.
        '''
        self.stopnow = True

    def run(self):
        self.filedb = self.fs.filedb

        while not self.stopnow:
            didSomething = False

            try:
                didSomething = self.replicateNextFile()
            except Exception, exc:
                self.fs.error("When replicating : %s" % (exc))

            # Sleep for one minute unless something happens..
            if not didSomething and self.fs.config["replicatorIdleTime"] > 0:
                self.fs.debug("Idling for %s seconds max..." % \
                              self.fs.config["replicatorIdleTime"], "repl")

                idle_secs = self.fs.config["replicatorIdleTime"]

                while not self.stopnow and idle_secs > 0:
                    time.sleep(1)
                    idle_secs -= 1
            else:
                time.sleep(self.fs.config["replicatorInterval"])

    def replicateNextFile(self):
        data = self.fs.selectFileToReplicate()

        self.fs.debug("File %s" % (data),"repl")

        if not data:
            return None

        # Do we have enough space to download the file?
        while self.fs.getFreeDisk() < data["size"]:
            maxKnFile = self.fs.filedb.getMaxKnInNode(self.fs.host)

            # No files to delete to make space!
            if not len(maxKnFile):
                return None

            maxKn = self.fs.filedb.getKn(maxKnFile[0])

            # No files with a high enough kn to make space!
            if maxKn < data["kn"] + 1:
                return None

            self.fs.debug("Deleting file %s because it has kn=%s" % \
                          (maxKnFile[0], maxKn), "repl")

            self.fs.deleteFile(maxKnFile[0])

        downloaded = self.fs.downloadFile(data["file"], data["nodes"])

        return downloaded
