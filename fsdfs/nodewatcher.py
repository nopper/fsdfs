from time import sleep, time
from threading import Thread

class NodeWatcher(Thread):
    def __init__(self, fs):
        Thread.__init__(self)
        self.daemon = True
        self.fs = fs

    def run(self):

        while True:
            sleep(self.fs.config["reportInterval"] / 2)

            for node in self.fs.filedb.listNodes():
                lastUpdate = self.fs.filedb.getNode(node)["lastUpdate"]

                reportInterval = self.fs.config["reportInterval"]
                maxMissedReports = self.fs.config["maxMissedReports"]

                maxThreshold = time() - reportInterval * maxMissedReports

                if lastUpdate < maxThreshold:
                    self.fs.debug("Node %s missed %s reports, removing it " \
                                  "from the swarm" % \
                                  (node, self.fs.config["maxMissedReports"]))
                    self.fs.filedb.removeNode(node)
