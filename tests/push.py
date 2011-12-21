#!/usr/bin/env python
# encoding: utf-8

import os, sys, re
import unittest
import threading
import shutil
import logging
logging.basicConfig(level=logging.DEBUG)
from time import sleep

sys.path.insert(0, os.path.join('.'))
from fsdfs.filesystem import Filesystem

class TestFS(Filesystem):
    _rules = {"n": 1}

    def getReplicationRules(self,file):
        return self._rules

class pushTests(unittest.TestCase):
    filedb ="memory"

    def setUp(self):
		if os.path.exists("./tests/datadirs"):
			shutil.rmtree("./tests/datadirs")
		os.makedirs("./tests/datadirs")

    def testPush(self):
        """
        We test the PUSH method which kindly ask to a remote node to download a
        specific file present on the DFS.
        """

        secret = "merz"

        nodeA = TestFS({
            "host":"localhost:52342",
            "datadir":"./tests/datadirs/A",
            "secret":secret,
            "resetFileDbOnStart":True,
            "master":"localhost:52342",
            "replicatorIdleTime":60,
            "maxstorage":10,
            "filedb":self.filedb,
            "garbageMinKn":-1
        })

        nodeB = TestFS({
            "host":"localhost:52352",
            "datadir":"./tests/datadirs/B",
            "secret":secret,
            "resetFileDbOnStart":True,
            "replicatorIdleTime":60,
            "master":"localhost:52342",
            "maxstorage":10,
            "filedb":self.filedb
        })

        nodeA.start()
        nodeB.start()

        nodeB.importFile("tests/fixtures/10b.txt","tests/fixtures/10b.txt")

        sleep(4)

        self.assertHasFile(nodeB, "tests/fixtures/10b.txt")
        self.assertHasFile(nodeA, "tests/fixtures/10b.txt", False)

        self.assertTrue(nodeB.pushFile("tests/fixtures/10b.txt"))

        self.assertHasFile(nodeB, "tests/fixtures/10b.txt")
        self.assertHasFile(nodeA, "tests/fixtures/10b.txt")

        nodeA.stop()
        nodeB.stop()

    def assertHasFile(self,node,destpath, present=True):
        if present:
            self.assertTrue(os.path.isfile(node.getLocalFilePath(destpath)))

            if os.path.isfile(node.getLocalFilePath(destpath)):
                self.assertEquals(open(node.getLocalFilePath(destpath)).read(),open(destpath).read())
        else:
            self.assertFalse(os.path.isfile(node.getLocalFilePath(destpath)))

if __name__ == '__main__':
    unittest.main()
