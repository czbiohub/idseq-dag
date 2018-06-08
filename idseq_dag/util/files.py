import multiprocessing
import os
import threading
import time
import traceback

import idseq_dag.util.command as command
import idseq_dag.util.log as log

uploader_status = {}
uploader_threads = []
# Peak network and storage perf for a typical small instance is saturated by
# just a few concurrent streams.
MAX_CONCURRENT_COPY_OPERATIONS = 8
iostream = multiprocessing.Semaphore(MAX_CONCURRENT_COPY_OPERATIONS)
# Make a second semaphore for uploads to reserve some capacity for downloads.
MAX_CONCURRENT_UPLOAD_OPERATIONS = 4
iostream_uploads = multiprocessing.Semaphore(MAX_CONCURRENT_UPLOAD_OPERATIONS)


def check_s3_presence(s3_path):
    ''' True if s3_path exists. False otherwise. '''
    try:
        o = command.execute_with_output("aws s3 ls %s" % s3_path)
        if o:
            return True
    except:
        pass
    return False


def check_s3_presence_for_file_list(s3_dir, file_list):
    for f in file_list:
        if not check_s3_presence(os.path.join(s3_dir, f)):
            return False
    return True


def touch_s3_file(s3_file_path):
    try:
        command.execute("aws s3 cp --metadata '{\"touched\":\"now\"}' %s %s" % (s3_file_path, s3_file_path))
        return True
    except:
        return False


def touch_s3_file_list(s3_dir, file_list):
    for f in file_list:
        touch_s3_file(os.path.join(s3_dir, f))


def fetch_genome(s3genome, strict=True, mutex=threading.RLock(), mutexes={}):  #pylint: disable=dangerous-default-value
    """Fetch and expand genome archive from s3 into local dir. Return that local
    dir. If already downloaded, return right away. If a fetch of the same genome
    is already in progress on another thread, wait for it to complete or fail;
    and if it failed, try again. If all tries fail, raise an exception (strict)
    or return None (not strict).

    Typical use:

        fruitfly_dir = fetch_genome("s3://fruitfly_genome.tar")
        # Prefetching optimization:  While doing compute intensive work on fruit flies,
        # start fetching the butterfly genome.
        threading.Thread(target=fetch_genome, args=["s3://butterfly_genome.tar"]).start()
        ... do some compute intensive work on fruit flies ...
        butterfly_dir = fetch_genome("s3://butterfly_genome.tar")
        threading.Thread(target=fetch_genome, args=["s3://firefly_genome.tar"]).start()
        ... do some compute intensive work on butterflies ...
        firefly_dir = fetch_genome("s3://firefly_genome.tar")
        ...

    Without the pre-fetching thread, the compute intensive work on butterflies
    would have to wait for the entire butterfly genome to be downloaded. With
    pre-fetching like this, the download of the butterfly genome proceeds in
    parallel with the fruit fly computation, and by the time the butterfly
    genome is needed, it may already have been fully downloaded, so the
    butterfly computation won't have to wait for it. Similarly, the download
    of the firefly genome proceeds in parallel with the butterfly computation,
    and the firefly genome will be ready by the time it's needed. The program
    would still work correctly if we comment out all the pre-fetching threads,
    but would take much longer to execute.

    It may be tempting to initiate all fetching parallel at the start of the
    program, but that's undesirable for two reasons:

      1) Fetching data with s3mi fully utilizes the I/O bandwidth of moderately
    sized instances, so fetching multiple streams in parallel will just slow them
    down and delay the moment we can begin computing.

      2) If the different computation stages support result caching, so that
      typically the first N stages would be cached from a previous run, and the
      computation would resume from stage N+1, this pattern beautifully avoids
      any unnecessary fetching of data that won't be needed for the cached
      stages, while still fetching the data needed for stage N+1. If, instead,
      we were to initiate pre-fetching at the beginning of the program, we would
      have to carefully ensure we only prefetch data that will in fact be
      needed, by replicating some of the caching logic.
    """
    with mutex:
        if s3genome not in mutexes:
            mutexes[s3genome] = threading.RLock()
        mx = mutexes[s3genome]
    with mx:
        return fetch_genome_work(s3genome, strict)


def fetch_genome_work(s3genome, strict):
    genome_name = os.path.basename(s3genome).rstrip(".gz").rstrip(".tar")
    if genome_name not in ("STAR_genome", "bowtie2_genome",
                           "hg38_pantro5_k16"):
        log.write("Oh hello interesting new genome {}".format(genome_name))
    # What to do about REF_DIR?
    genome_dir = os.path.join(REF_DIR, genome_name)

    if not os.path.exists(genome_dir):
        # Can consider merging with fetch_reference: idseq-pipeline/issues/223
        try:
            install_s3mi()
            tarfile = uncompressed(s3genome)
            try:
                log.write("Trying to download compressed genome...")
                cmd = "s3mi cat {tarfile} | tar xvf - -C {refdir}".format(
                    tarfile=tarfile, refdir=REF_DIR)
                command.execute(cmd)
                assert os.path.isdir(genome_dir)
            except:
                if tarfile != s3genome:
                    print("Uncompressed version doesn't exist. Downloading "
                          "compressed version...")
                    # The uncompressed version doesn't exist. This is much
                    # slower, but no choice.
                    command.execute("rm -rf {}".format(genome_dir))
                    cmd = "s3mi cat {s3genome} | tar xvfz - -C {refdir}".format(
                        s3genome=s3genome, refdir=REF_DIR)
                    command.execute(cmd)
                    assert os.path.isdir(genome_dir)
                else:
                    # Okay, may be s3mi is broken.  We'll try aws cp next.
                    log.write("Error in downloading with s3mi. Trying aws cp...")
                    raise
        except:
            try:
                command.execute("rm -rf {}".format(genome_dir))
                cmd = "aws s3 cp --quiet {s3genome} - | tar xvf - -C {refdir}".format(
                    s3genome=s3genome, refdir=REF_DIR)
                command.execute(cmd)
                assert os.path.isdir(genome_dir)
            except:
                msg = "Failed to download index {}, it might not exist.".format(
                    s3genome)
                log.write(msg)
                if strict:
                    raise
                genome_dir = None
                # Note we do not reraise the exception here, just print it.
                traceback.print_exc()
        if genome_dir:
            log.write("successfully downloaded index {}".format(s3genome))
    return genome_dir


def install_s3mi(installed={}, mutex=threading.RLock()):  #pylint: disable=dangerous-default-value
    with mutex:
        if installed:  # Mutable default value persists
            return
        try:
            # This is typically a no-op.
            command.execute(
                "which s3mi || pip install git+git://github.com/chanzuckerberg/s3mi.git"
            )
            command.execute(
                "s3mi tweak-vm || echo s3mi tweak-vm is impossible under docker. Continuing..."
            )
        finally:
            installed['time'] = time.time()


def uncompressed(s3genome):
    if s3genome.endswith(".gz"):
        return s3genome[:-3]
    if s3genome.endswith(".tgz"):
        return s3genome[:-3] + "tar"
    return s3genome


def uploader_start(from_f, to_f):
    t = threading.Thread(
        target=upload, args=[from_f, to_f, uploader_status])
    t.start()
    uploader_threads.append(t)


def uploader_check_wait_all():
    for t in uploader_threads:
        t.join()
    for filename, status in uploader_status.items():
        msg = "Bad upload status {} for file {}".format(status, filename)
        assert status == "success", msg


@command.retry
def upload_with_retries(from_f, to_f):
    command.execute("aws s3 cp --quiet {from_f} {to_f}".format(
        from_f=from_f, to_f=to_f))


def upload(from_f, to_f, status, status_lock=threading.RLock()):
    try:
        with iostream_uploads:  # Limit concurrent uploads so as not to stall the pipeline.
            with iostream:  # Still counts toward the general semaphore.
                upload_with_retries(from_f, to_f)
            with status_lock:
                status[from_f] = "success"
    except:
        with status_lock:
            status[from_f] = "error"
        raise
