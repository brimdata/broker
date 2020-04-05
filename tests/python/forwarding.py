import unittest
import multiprocessing
import sys
import time

import broker

# Set this to true when investigation test failures.
debugOutput = False

def printDebug(line):
    if debugOutput:
        print(line)

def setUpPeers(self, create_s1 = True, create_s2 = True, create_s3 = True,
               create_s4 = True):
    # Disable SSL. We have other unit tests to check whether SSL works.
    def cfg():
        opts = broker.BrokerOptions()
        opts.disable_ssl = True
        # opts.ignore_broker_conf = True
        return broker.Configuration(opts)

    printDebug('create endpoints')
    self.ep1 = broker.Endpoint(cfg())
    self.ep2 = broker.Endpoint(cfg())
    self.ep3 = broker.Endpoint(cfg())
    self.ep4 = broker.Endpoint(cfg())

    printDebug('create subscribers')
    if create_s1: self.s1 = self.ep1.make_subscriber("/test/")
    if create_s2: self.s2 = self.ep2.make_subscriber("/test/")
    if create_s3: self.s3 = self.ep3.make_subscriber("/test/")
    if create_s4: self.s4 = self.ep4.make_subscriber("/test/")
    ssubs = [self.ep1.make_status_subscriber(True),
             self.ep2.make_status_subscriber(True),
             self.ep3.make_status_subscriber(True),
             self.ep4.make_status_subscriber(True)]

    printDebug('start listening on endpoints 2-4')
    p2 = self.ep2.listen("127.0.0.1", 0)
    p3 = self.ep3.listen("127.0.0.1", 0)
    p4 = self.ep4.listen("127.0.0.1", 0)

    printDebug('establish peering relations: ep1 <-> ep2 <-> ep3 <-> ep4')
    self.ep1.peer("127.0.0.1", p2, 1.0)
    self.ep2.peer("127.0.0.1", p3, 1.0)
    self.ep3.peer("127.0.0.1", p4, 1.0)

    for ssub in ssubs:
        for i in [1, 2, 3]:
            x = ssub.get()
            printDebug(str(x))
            if x.code() != broker.SC.PeerAdded:
                raise RuntimeError('unexpected status: ' + str(x))

class TestCommunication(unittest.TestCase):

    def tearDown(self):
        self.ep1.shutdown()
        self.ep2.shutdown()
        self.ep3.shutdown()
        self.ep4.shutdown()

    def test_two_hops(self):
        setUpPeers(self)

        printDebug('publish data on ep1 and ep4')
        self.ep1.publish("/test/foo", "Foo!")
        self.ep4.publish("/test/bar", "Bar!")

        printDebug('receive data on ep1')
        x = self.s1.get()
        self.assertEqual(x, ('/test/bar', 'Bar!'))

        printDebug('receive data on ep4')
        x = self.s4.get()
        self.assertEqual(x, ('/test/foo', 'Foo!'))

    def test_two_hops_with_forward(self):
        # Only ep 1 & 4 have subscribers, but they can route through ep 2 & 3.
        setUpPeers(self, create_s2 = False, create_s3 = False)

        printDebug('publish data on ep1 and ep4')
        self.ep1.publish("/test/foo", "Foo!")
        self.ep4.publish("/test/bar", "Bar!")

        printDebug('receive data on ep1')
        x = self.s1.get()
        self.assertEqual(x, ('/test/bar', 'Bar!'))

        printDebug('receive data on ep4')
        x = self.s4.get()
        self.assertEqual(x, ('/test/foo', 'Foo!'))

if __name__ == '__main__':
    unittest.main(verbosity=3)
