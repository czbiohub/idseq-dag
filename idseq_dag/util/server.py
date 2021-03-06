import os
import json
import time
import random
import threading
import traceback
import multiprocessing

import idseq_dag.util.command as command
import idseq_dag.util.log as log


MIN_INTERVAL_BETWEEN_DESCRIBE_INSTANCES = 180
MAX_INTERVAL_BETWEEN_DESCRIBE_INSTANCES = 900
MAX_POLLING_LATENCY = 10  # seconds
MAX_INSTANCES_TO_POLL = 8
MAX_DISPATCHES_PER_MINUTE = 10

@command.retry
def get_server_ips_work(service_name, environment):
    tag = "service"
    value = "%s-%s" % (service_name, environment)
    describe_json = json.loads(
        command.execute_with_output(
            "aws ec2 describe-instances --filters 'Name=tag:%s,Values=%s' 'Name=instance-state-name,Values=running'"
            % (tag, value)))
    server_ips = [
        instance["NetworkInterfaces"][0]["PrivateIpAddress"]
        for reservation in describe_json["Reservations"] 
        for instance in reservation["Instances"]
    ]
    
    return server_ips


def get_server_ips(service_name,
                   environment,
                   aggressive=False,
                   cache={},
                   mutex=threading.RLock()):  #pylint: disable=dangerous-default-value
    try:
        with mutex:
            if aggressive:
                period = MIN_INTERVAL_BETWEEN_DESCRIBE_INSTANCES
            else:
                period = MAX_INTERVAL_BETWEEN_DESCRIBE_INSTANCES
            now = time.time()
            cache_key = (service_name, environment)
            if cache_key not in cache or now - cache[cache_key][0] >= period:
                # this may raise an exception when the AWS account rate limit is exceeded due to many concurrent jobs
                cache[cache_key] = (now,
                                    get_server_ips_work(
                                        service_name, environment))
            return cache[cache_key][1]
    except:
        # return [] causes a sleep of wait_seconds before retrying (see below)
        traceback.print_exc()
        return []


def wait_for_server_ip_work(service_name,
                            key_path,
                            remote_username,
                            environment,
                            max_concurrent,
                            chunk_id,
                            had_to_wait=[False]):  #pylint: disable=dangerous-default-value
    while True:
        log.write(
            "Chunk {chunk_id} of {service_name} is at third gate".format(
                chunk_id=chunk_id, service_name=service_name))
        instance_ips = get_server_ips(
            service_name, environment, aggressive=had_to_wait[0])
        instance_ips = random.sample(instance_ips,
                                     min(MAX_INSTANCES_TO_POLL,
                                         len(instance_ips)))
        ip_nproc_dict = {}
        dict_mutex = threading.RLock()
        dict_writable = True

        def poll_server(ip):
            # ServerAliveInterval to fix issue with containers keeping open
            # an SSH connection even after worker machines had finished
            # running.
            commands = "ps aux | grep %s | grep -v bash || echo error" % service_name
            output = command.execute_with_output(
                command.remote(commands, key_path, remote_username, ip),
                timeout=MAX_POLLING_LATENCY).rstrip().split("\n")
            if output != ["error"]:
                with dict_mutex:
                    if dict_writable:
                        ip_nproc_dict[ip] = len(output) - 1

        poller_threads = []
        for ip in instance_ips:
            pt = threading.Thread(target=poll_server, args=[ip])
            pt.start()
            poller_threads.append(pt)
        for pt in poller_threads:
            pt.join(MAX_POLLING_LATENCY)
        with dict_mutex:
            # Any zombie threads won't be allowed to modify the dict.
            dict_writable = False
        if len(ip_nproc_dict) < len(poller_threads):
            log.write(
                "Only {} out of {} instances responded to polling;  {} threads are timing out.".
                format(
                    len(ip_nproc_dict), len(poller_threads),
                    len([pt for pt in poller_threads if pt.isAlive()])))
        log.write(
            "Chunk {chunk_id} of {service_name} is at fourth gate".format(
                chunk_id=chunk_id, service_name=service_name))
        if not ip_nproc_dict:
            have_capacity = False
        else:
            min_nproc_ip = min(ip_nproc_dict, key=ip_nproc_dict.get)
            min_nproc = ip_nproc_dict[min_nproc_ip]
            have_capacity = (min_nproc < max_concurrent)
        if have_capacity:
            had_to_wait[0] = False
            # Make an urn where each ip occurs more times if it has more free slots, and not at all if it lacks free slots
            urn = []
            for ip, nproc in ip_nproc_dict.items():
                free_slots = max_concurrent - nproc
                if free_slots > 0:
                    weight = 2**free_slots - 1
                    urn.extend([ip] * weight)
            min_nproc_ip = random.choice(urn)
            free_slots = max_concurrent - ip_nproc_dict[min_nproc_ip]
            log.write("%s server %s has capacity %d. Kicking off " %
                         (service_name, min_nproc_ip, free_slots))
            return min_nproc_ip
        else:
            had_to_wait[0] = True
            wait_seconds = random.randint(
                max(20, MAX_POLLING_LATENCY), max(60, MAX_POLLING_LATENCY))
            log.write("%s servers busy. Wait for %d seconds" %
                         (service_name, wait_seconds))
            time.sleep(wait_seconds)


def wait_for_server_ip(service_name,
                       key_path,
                       remote_username,
                       environment,
                       max_concurrent,
                       chunk_id,
                       mutex=threading.RLock(),
                       mutexes={},
                       last_checks={}):  #pylint: disable=dangerous-default-value
    # We rate limit these to ensure fairness across jobs regardless of job size
    if service_name == 'rapsearch2':
        # TODO: remove this duct tape
        service_name = 'rapsearch'
    with mutex:
        if service_name not in mutexes:
            mutexes[service_name] = threading.RLock()
            last_checks[service_name] = [None]
        lc = last_checks[service_name]
        mx = mutexes[service_name]
    log.write("Chunk {chunk_id} of {service_name} is at second gate".format(
        chunk_id=chunk_id, service_name=service_name))
    with mx:
        if lc[0] is not None:
            sleep_time = (60.0 / MAX_DISPATCHES_PER_MINUTE) - (
                time.time() - lc[0])
            if sleep_time > 0:
                log.write(
                    "Sleeping for {:3.1f} seconds to rate-limit wait_for_server_ip.".
                    format(sleep_time))
                time.sleep(sleep_time)
        lc[0] = time.time()
        # if we had to wait here, that counts toward the rate limit delay
        result = wait_for_server_ip_work(service_name, key_path,
                                         remote_username, environment,
                                         max_concurrent, chunk_id)
        return result

