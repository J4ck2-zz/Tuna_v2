import subprocess

from fabric import task

from alibaba.instance import InstanceManager
from alibaba.remote import Bench
from benchmark.commands import CommandMaker
from benchmark.local import LocalBench
from benchmark.logs import LogParser, ParseError
from benchmark.utils import BenchError, Print


@task
def local(ctx):
    """Run benchmarks on localhost"""
    bench_params = {
        "nodes": [4],
        "duration": 10,
        "rate": 1000,  # tx send rate
        "batch_size": 1024,  # the max number of tx that can be hold
        "log_level": 0b1111,  # 0x1 infolevel 0x2 debuglevel 0x4 warnlevel 0x8 errorlevel
    }
    node_params = {
        "pool": {
            # "rate": 1_000,              # ignore: tx send rate
            "tx_size": 8,  # tx size
            # "batch_size": 200,          # ignore: the max number of tx that can be hold
            "max_queue_size": 10_000,
        },
        "consensus": {
            "leader_count": 2,
            "leader_timeout_ms": 300,
            "min_block_delay_ms": 20,
            "sync_interval_ms": 100,
            "max_payloads_per_block": 32,
            "cache_rounds": 100,
        },
        "mempool": {
            "sync_timeout": 1000,  # node sync time
            "retry_delay": 1000,  # request block period
            "payload_delay_send": 0,  # send delay self payload
            "Requst_Pload_delay": 200,  # send requestpayloadmsg delay
            "maxmempoolqueensize": 10_000,
        },
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug=True).result()
        print(ret)
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=1):
    """Create a testbed"""
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    """Destroy the testbed"""
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def cleansecurity(ctx):
    """Destroy the testbed"""
    try:
        InstanceManager.make().delete_security()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=10):
    """Start at most `max` machines per data center"""
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    """Stop all machines"""
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def uploadexec(ctx):
    try:
        Bench(ctx).upload_exec()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    """Display connect information about all the available machines"""
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx):
    """Run Tuna_v2 consensus benchmarks on Alibaba Cloud"""
    bench_params = {
        "nodes": [5],
        "node_instance": 2,  # the number of running instance for a node  (max = 4)
        "duration": 60,
        "rate": [10000],  # tx send rate
        "batch_size": 1024,  # the max number of tx that can be hold
        "log_level": 0b1111,  # 0x1 infolevel 0x2 debuglevel 0x4 warnlevel 0x8 errorlevel
        "runs": 1,
    }
    node_params = {
        "pool": {
            # "rate": 1_000,              # ignore: tx send rate
            "tx_size": 16,  # tx size
            # "batch_size": 200,          # ignore: the max number of tx that can be hold
            "max_queue_size": 10_000,
        },
        "consensus": {
            "leader_count": 2,
            "leader_timeout_ms": 300,
            "min_block_delay_ms": 20,
            "sync_interval_ms": 100,
            "max_payloads_per_block": 32,
            "cache_rounds": 100,
        },
        "mempool": {
            "sync_timeout": 8_000,  # node sync time
            "retry_delay": 10_000,  # request block period
            "payload_delay_send": 80,
            "Requst_Pload_delay": 800,
            "maxmempoolqueensize": 10_000,
        },
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug=False)
    except BenchError as e:
        Print.error(e)


@task
def kill(ctx):
    """Stop node processes on all machines"""
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def download(ctx, node_instance=2, ts="2025-08-14-18-45-48"):
    """download logs"""
    try:
        print(Bench(ctx).download(node_instance, ts).result())
    except BenchError as e:
        Print.error(e)


@task
def clean(ctx):
    cmd = f"{CommandMaker.cleanup_configs()};rm -f main"
    subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)


@task
def logs(ctx):
    """Print a summary of the logs"""
    # try:
    print(LogParser.process("./logs/2025-08-14-18-45-48").result())
    # except ParseError as e:
    #     Print.error(BenchError('Failed to parse logs', e))
