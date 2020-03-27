
import unittest
import multiprocessing
import sys
import time

import broker

def setup_peers(create_s1=True, create_s2=True, create_s3=True, create_s4=True):
    # disable SSL, since we have other unit tests to check whether SSL works.
    def cfg():
        opts = broker.BrokerOptions()
        opts.disable_ssl = True
        opts.ignore_broker_conf = True
        return broker.Configuration(opts)
    ep1 = broker.Endpoint(cfg())
    ep2 = broker.Endpoint(cfg())
    ep3 = broker.Endpoint(cfg())
    ep4 = broker.Endpoint(cfg())

    s1 = ep1.make_subscriber("/test/") if create_s1 else None
    s2 = ep2.make_subscriber("/test/") if create_s2 else None
    s3 = ep3.make_subscriber("/test/") if create_s3 else None
    s4 = ep4.make_subscriber("/test/") if create_s4 else None

    p2 = ep2.listen("127.0.0.1", 0)
    p3 = ep3.listen("127.0.0.1", 0)
    p4 = ep4.listen("127.0.0.1", 0)

    # ep1 <-> ep2 <-> ep3 <-> ep4
    ep1.peer("127.0.0.1", p2, 1.0)
    ep2.peer("127.0.0.1", p3, 1.0)
    ep3.peer("127.0.0.1", p4, 1.0)

    return ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4))

class TestCommunication(unittest.TestCase):
    def test_two_hops(self):
        # All hops have local subscribers.
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers()

        ep1.publish("/test/foo", "Foo!")
        ep4.publish("/test/bar", "Bar!")

        x = s4.get()
        self.assertEqual(x, ('/test/foo', 'Foo!'))

        x = s1.get()
        self.assertEqual(x, ('/test/bar', 'Bar!'))

    def test_two_hops_with_forward(self):
        # Only ep 1 & 4 have subscribers, but they can route through ep 2 & 3.
        ((ep1, ep2, ep3, ep4), (s1, s2, s3, s4)) = setup_peers(create_s2=False, create_s3=False)

        ep1.publish("/test/foo", "Foo!")
        ep4.publish("/test/bar", "Bar!")

        x = s4.get()
        self.assertEqual(x, ('/test/foo', 'Foo!'))
        x = s1.get()
        self.assertEqual(x, ('/test/bar', 'Bar!'))

if __name__ == '__main__':
    unittest.main(verbosity=3)
