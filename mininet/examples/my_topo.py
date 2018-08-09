#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.node import CPULimitedHost, Host, Node
from mininet.node import OVSKernelSwitch
from mininet.node import IVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink, Intf
from subprocess import call
from random import randint
from mininet.util import macColonHex
from pymongo import MongoClient
from uuid import uuid4
import copy
import random

switch_start = 0   # switch startls
switch_end = 10    # switch end   <=254
host_start = 1     #host ip start  >=1
host_end = 10      # host ip end <=254


def get_mac():
    Maclist = []
    for i in range(1, 7):
        RANDSTR = "".join(random.sample("0123456789abcdef", 2))
        Maclist.append(RANDSTR)
    RANDMAC = ":".join(Maclist)
    return RANDMAC


def myNetwork():
    mongodb = MongoClient("127.0.0.1")
    db_handler = mongodb.ncr.mac_ip
    db_handler.remove()
    net = Mininet(topo=None,
                  build=False, autoSetMacs=False,
                  ipBase='10.0.0.0/16', inNamespace=False)

    # Set ovs-controller host_port
    info('*** Adding controller\n')
    c0 = RemoteController('c0', ip='127.0.0.1', port=6633)
    net.addController(c0)

    # Add switch ,start with switch_start, end with switch_end
    info('*** Add switches\n')
    switchs = []
    for i in range(switch_start, switch_end + 1):
        switchs.append(net.addSwitch("s%s" % i, cls=OVSKernelSwitch))

    # Add a switch as physical sdn , and name it "kernel_switch" .
    kernel_switch = net.addSwitch("s%s" % str(switch_end + 1), cls=OVSKernelSwitch)
    info("switchs=%s" % switchs)

    info('*** Add hosts\n')
    hosts = []
    times = 0
    # Loop switch_start to switch_end, generation host
    for i in range(switch_start, switch_end + 1):
        for j in range(host_start, host_end + 1):
            # switchs = switchs[randint(switch_start, switch_end)]
            ip = "10.0.%s.%s" % (i, j)
            mac_base = int(ip.replace(".",""))
            mac = macColonHex(mac_base)
            hosts.append(net.addHost('t%s_h%s' % (i, j), ip=ip, mac=mac, defaultRoute=None))
            a = hosts[-1]
            times += 1
            # Simulate data and insert in to mac_ip tables
            db_handler.insert_one({"tunnel_key": str(i), "ip": ip, "mac": mac})

    info("times=%s, hosts=%s" % (times, hosts))

    # add link to the host and connect to a switch(random)
    for host in hosts:
        # print randint
        net.addLink(host, switchs[randint(switch_start, switch_end)])

    #Link all switch with kernel_switch
    for switch in switchs:
        net.addLink(switch, kernel_switch)

    # Build the net
    info('*** Starting network\n')
    net.build()
    info('*** Starting controllers\n')

    # Start all link
    for switch in switchs:
        switch.start([c0])
    kernel_switch.start([c0])
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    myNetwork()
