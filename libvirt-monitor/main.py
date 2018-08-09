import libvirt
import paramiko
import time
import logging
import datetime
from xml.etree import ElementTree
from pymongo import MongoClient
from threading import Thread

logging.basicConfig(filename="/var/log/racelog/host_libvirt_monitor.log", level=logging.INFO, filemode='a+',
                    format=('%(asctime)s - %(levelname)s: %(message)s'))
LOG = logging.getLogger(__name__)


class Monitor(object):
    def __init__(self):
        self.libvirt_conns = {}
        self.db = self.get_mongo()
        self.ssh_retry = False
        self.libvirt_retry = False

    def get_mongo(self):
        db = MongoClient("127.0.0.1")
        return db.race

    def get_hosts(self):
        nodes = []
        f = open("/etc/hosts")
        for host in f.readlines():
            sp = host.split()
            if len(sp) == 2 and "node" in sp[1]:
                nodes.append(sp[1])
        return nodes

    def connect_host_libvirt(self, host):
        conn = libvirt.open("qemu+ssh://root@%s/system" % host)
        return conn

    def connect_host(self, host):
        ssh_c = paramiko.client.SSHClient()
        ssh_c.load_system_host_keys()
        ssh_c.connect(host)
        return ssh_c

    def get_all_vm(self, host, conn):
        domains = []
        domain_ids = conn.listDomainsID()
        for id in domain_ids:
            domain = conn.lookupByID(id)
            domains.append(domain)
        return domains

    def get_vm_cpu_usage(self, domain, host):
        try:
            t1 = time.time()
            c1 = int(domain.info()[4])
            time.sleep(1)
            t2 = time.time()
            c2 = int(domain.info()[4])
            c_nums = int(domain.info()[3])
            usage = (c2 - c1) * 100 / ((t2 - t1) * c_nums * 1e9)
        except Exception as e:
            self.libvirt_retry = True
            LOG.error("Get vm cpu faild, host=%s ,domain=%s: %s " % (host, domain.name(), e.message))
            return
        if usage == 0:
            self.get_vm_cpu_usage(domain, host)
        else:
            self.save_vm_cpu_usage(usage, domain, host)

    def save_vm_cpu_usage(self, usage, domain, host):
        last_record = {
            "node":host,
            "domain":domain.name(),
            "usage":usage,
            "create_time": datetime.datetime.now()
        }
        self.db.vmcpu_usage.insert_one(
            last_record
        )

    def get_vm_network_usage(self, domain, host):
        raw_xml = domain.XMLDesc(0)
        xl = ElementTree.fromstring(raw_xml)
        interfaces = [i.get("dev") for i in xl.findall("devices/interface/target")]
        for inter in interfaces:
            try:
                stats = domain.interfaceStats(inter)
            except Exception as e:
                self.libvirt_retry = True
                LOG.error("Get vm network faild, host=%s ,domain=%s: %s " % (host, domain.name(), e.message))
                return
            self.save_vm_network_usage(inter, stats, domain, host)

    def save_vm_network_usage(self, inter, stats, domain, host):
        usage = {"rd_req": stats[1], "rd_bytes": stats[0], "wr_req": stats[5], "wr_write": stats[4]}
        last_record = {
            "node": host,
            "domain": domain.name(),
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.vmnetwork_usage.insert_one(
            last_record
        )

    def get_vm_blcok_usage(self, domain, host):
        # stats = []
        raw_xml = domain.XMLDesc(0)
        xl = ElementTree.fromstring(raw_xml)
        files = [i.get("file") for i in xl.findall("devices/disk/source")]
        for file in files:
            try:
                stats = domain.blockStats(file)
            except Exception as e:
                self.libvirt_retry = True
                LOG.error("Get vm block faild, host=%s ,domain=%s: %s " % (host, domain.name(), e.message))
                return
            self.save_vm_block_usage(file, stats, domain, host)

        # print stats

    def save_vm_block_usage(self, file, stats, domain, host):
        usage = {"rd_req": stats[0], "rd_bytes": stats[1], "wr_req": stats[2], "wr_write": stats[3]}
        last_record = {
            "node": host,
            "domain": domain.name(),
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.vmblock_usage.insert_one(
            last_record
        )

    def get_host_diskio_usage(self, ssh_c, host):
        cmd = "python -c \"import psutil; print dict(psutil.disk_io_counters()._asdict())\""
        try:
            stdin, stdout, stderr = ssh_c.exec_command(cmd)
        except Exception as e:
            self.ssh_retry = True
            LOG.error("Get host disk io usage faild ,host=%s: %s " % (host, e.message))
            return
        for out in stdout.readlines():
            out = eval(out)
            diskio_usage = out
            self.save_host_disk_io_usage(host, diskio_usage)

    def save_host_disk_io_usage(self, host, usage):
        last_record = {
            "node": host,
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.hostdiskio_usage.insert_one(
            last_record
        )

    def get_host_net_usage(self, ssh_c, host):
        cmd = "python -c \"import psutil; print dict(psutil.net_io_counters()._asdict())\""
        try:
            stdin, stdout, stderr = ssh_c.exec_command(cmd)
        except Exception as e:
            self.ssh_retry = True
            LOG.error("Get host net io usage faild ,host=%s: %s " % (host, e.message))
            return
        for out in stdout.readlines():
            out = eval(out)
            net_usage = out
            self.save_host_net_usage(host, net_usage)

    def save_host_net_usage(self, host, usage):
        last_record = {
            "node": host,
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.hostnet_usage.insert_one(
            last_record
        )

    def get_host_swap_usage(self, ssh_c, host):
        cmd = "python -c \"import psutil; print dict(psutil.swap_memory()._asdict())\""
        try:
            stdin, stdout, stderr = ssh_c.exec_command(cmd)
        except Exception as e:
            self.ssh_retry = True
            LOG.error("Get host swap usage faild ,host=%s: %s " % (host, e.message))
            return
        for out in stdout.readlines():
            out = eval(out)
            swap_usage = out
            self.save_host_swap_usage(host, swap_usage)

    def save_host_swap_usage(self, host, usage):
        last_record = {
            "node": host,
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.hostswap_usage.insert_one(
            last_record
        )

    def get_host_memory_usage(self, ssh_c, host):
        cmd = "python -c \"import psutil; print dict(psutil.virtual_memory()._asdict())\""
        try:
            stdin, stdout, stderr = ssh_c.exec_command(cmd)
        except Exception as e:
            self.ssh_retry = True
            LOG.error("Get host memory usage faild ,host=%s: %s " % (host, e.message))
            return
        for out in stdout.readlines():
            out = eval(out)
            memory_usage = out
            self.save_host_memory_usage(host, memory_usage)

    def save_host_memory_usage(self, host, usage):
        last_record = {
            "node": host,
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.hostmemory_usage.insert_one(
            last_record
        )

    def get_host_cpu_usage(self, ssh_c, host):
        cmd = "python -c \"import psutil; print psutil.cpu_percent(interval=1,percpu=True)\""
        try:
            stdin, stdout, stderr = ssh_c.exec_command(cmd)
        except Exception as e:
            self.ssh_retry = True
            LOG.error("Get host cpu usage faild ,host=%s: %s " % (host, e.message))
            return
        for out in stdout.readlines():
            out = eval(out)
            cpu_usage = out
            self.save_host_cpu_usage(host, cpu_usage)

    def save_host_cpu_usage(self, host, usage):
        last_record = {
            "node": host,
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.hostcpu_usage.insert_one(
            last_record
        )

    def get_host_disk_usage(self, ssh_c, host):
        cmd = "python -c \"import psutil; print dict(psutil.disk_usage('/')._asdict())\""
        try:
            stdin, stdout, stderr = ssh_c.exec_command(cmd)
        except Exception as e:
            self.ssh_retry = True
            LOG.error("Get host memory disk faild ,host=%s: %s " % (host, e.message))
            return
        for out in stdout.readlines():
            out = eval(out)
            disk_usage = {
                "total": out["total"],
                "used": out["used"],
                "percent": out["percent"],
                "free": out["free"]
            }
            # print "disk",out
            self.save_host_disk_usage(host, disk_usage)

    def save_host_disk_usage(self, host, usage):
        last_record = {
            "node": host,
            "usage": usage,
            "create_time": datetime.datetime.now()
        }
        self.db.hostdisk_usage.insert_one(
            last_record
        )

    def clear_timeout_data(self):
        one_hours_ago = datetime.datetime.now() - datetime.timedelta(seconds=3600)
        print self.db.vmnetwork_usage.find({"create_time":{"$lt": one_hours_ago}}).count()
        self.db.vmblock_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.vmcpu_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.vmnetwork_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.hostcpu_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.hostdisk_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.hostdiskio_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.hostmemory_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.hostnet_usage.delete_many({"create_time":{"$lt": one_hours_ago}})
        self.db.hostswap_usage.delete_many({"create_time":{"$lt": one_hours_ago}})

    def start_vm_monitor(self):
        LOG.info("Virtual machine monitor start")
        while True:
            threads = []
            hosts = self.get_hosts()
            for i in self.libvirt_conns:
                if not i in hosts:
                    del self.libvirt_conns[i]
                    LOG.info("found a host delete in /etc/hosts , connect delete, host=%s" % i)
            if hosts != self.libvirt_conns.keys():
                for host in hosts:
                    try:
                        if not host in self.libvirt_conns or self.ssh_retry:
                            self.libvirt_conns[host] = self.connect_host_libvirt(host)
                    except Exception as e:
                        LOG.error("connect libvirt %s faild: %s" % (host, e.message))
                        self.libvirt_retry = True
            if len(hosts) == len(self.libvirt_conns):
                self.ssh_retry = False
            for host, conn in self.libvirt_conns.items():
                try:
                    all_vm = self.get_all_vm(host, conn)
                except Exception as e:
                    self.ssh_retry = True
                    continue
                for vm in all_vm:
                    try:
                        threads.append(Thread(target=self.get_vm_cpu_usage, args=(vm, host)))
                        threads.append(Thread(target=self.get_vm_blcok_usage, args=(vm, host)))
                        threads.append(Thread(target=self.get_vm_network_usage, args=(vm, host)))
                    except Exception as e:
                        print e
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            time.sleep(14)

    def start_host_monitor(self):
        LOG.info("Compute host monitor start")
        host_connections = {}
        while True:
            hosts = self.get_hosts()
            # if delete  host
            for i in host_connections:
                if not i in hosts:
                    del host_connections[i]

            for host in hosts:
                try:
                    # if add host or self.retry=True
                    if not host in host_connections or self.ssh_retry:
                        host_connections[host] = self.connect_host(host)
                except Exception as e:
                    LOG.error("connect ssh %s faild: %s" % (host, e.message))
                    self.ssh_retry = True
            if len(hosts) == len(host_connections):
                self.ssh_retry = False
            threads = []
            for host, ssh_c in host_connections.items():
                threads.append(Thread(target=self.get_host_disk_usage, args=(ssh_c, host,)))
                threads.append(Thread(target=self.get_host_cpu_usage, args=(ssh_c, host,)))
                threads.append(Thread(target=self.get_host_memory_usage, args=(ssh_c, host,)))
                threads.append(Thread(target=self.get_host_swap_usage, args=(ssh_c, host,)))
                threads.append(Thread(target=self.get_host_net_usage, args=(ssh_c, host,)))
                threads.append(Thread(target=self.get_host_diskio_usage, args=(ssh_c, host,)))
                threads.append(Thread(target=self.clear_timeout_data))
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            time.sleep(14)

    def start(self):
        host_t = Thread(target=self.start_host_monitor)
        host_v = Thread(target=self.start_vm_monitor)
        host_t.start()
        host_v.start()
        host_t.join()
        host_v.join()


if __name__ == '__main__':
    m = Monitor()
    m.start()
