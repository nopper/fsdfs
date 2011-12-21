from time import sleep
from threading import Thread

class Reporter(Thread):
    def __init__(self, fs):
        Thread.__init__(self)
        self.fs = fs
        self.stopnow = False

    def run(self):
        first = True

        while not self.stopnow:
            try:
                if first and not self.fs.ismaster:
                    self.fs.report({
                        "all": list(self.fs.filedb.listInNode(self.fs.host))
                    })
                else:
                    self.fs.report()
                first = False
            except Exception, exc:
                self.fs.error("While reporting : %s" % exc)

            idle_secs = self.fs.config["reportInterval"]

            while not self.stopnow and idle_secs > 0:
                sleep(1)
                idle_secs -= 1

    def shutdown(self):
        self.stopnow = True
