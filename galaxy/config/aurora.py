"""
Aurora Job Runner
"""

import glob
import json
import logging
import os
import string
import subprocess
import time

from galaxy import model
from galaxy.jobs.runners import AsynchronousJobState, AsynchronousJobRunner

log = logging.getLogger( __name__ )

__all__ = [ 'AuroraJobRunner' ]

###############################################################################

DEFAULT_AURORA_CONTAINER = 'python:2.7'

AURORA_JOB_CONF_TEMPL = '''\
proc${id} = Process(
    name="proc${id}",
    cmdline="""
${cmdline}
""")
task${id} = Task(
    name = "task${id}",
    processes = [ proc${id} ],
    resources = Resources(cpu = ${cpu}, ram = ${ram_gb}*GB, disk = ${disk_gb}*GB)
)
job${id} = Job(
    cluster = "devcluster", environment = "devel", role = "galaxy",
    name = "job${id}",
    task = task${id},
    container = Container(docker = Docker(image = "${container}"))
)
jobs = [ job${id} ]
'''

AURORA_JOB_CMD_TEMPL = '''\
aurora job ${command} devcluster/galaxy/devel/job${id} ${option}
'''

SPLIT_CMD_TEMPL = '''\
 && fastq-splitter.pl --n-parts 16 ${datafile}
'''

TOPHAT_DEFAULT_PARALLEL_NUM = 16

###############################################################################
from sqlalchemy import Column, DateTime, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import BigInteger
import datetime
now = datetime.datetime.utcnow

class AuroraJobTable(declarative_base()):
    __tablename__ = "aurora_job"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    job_key = Column("job_key", String( 255 ), unique=True, nullable=False)
    job_id = Column("job_id", Integer, nullable=False)
    fragment_no = Column("fragment_no", Integer, nullable=True)
    num_cpus = Column("num_cpus", Float, nullable=False)
    ram_mb = Column("ram_mb", Integer, nullable=False)
    disk_mb = Column("disk_mb", Integer, nullable=False)
    slave_host = Column("slave_host", String( 255 ), nullable=False)
    docker_image = Column("docker_image", String( 255 ), nullable=False)
    starting_time = Column("starting_time", BigInteger, nullable=False)
    finished_time = Column("finished_time", BigInteger, nullable=False)
    create_time = Column("create_time", DateTime, default=now)
    update_time = Column("update_time", DateTime, default=now, onupdate=now)

###############################################################################

class AuroraJobState( AsynchronousJobState ):
    def __init__( self, **kwargs ):
        super( AuroraJobState, self ).__init__( **kwargs )
        self.failed = False
        self.container = DEFAULT_AURORA_CONTAINER
        self.job_annotation = 'none'
        self.aurora_job_statuses = {}

class AuroraJobRunner( AsynchronousJobRunner ):
    runner_name = "AuroraRunner"

    def __init__( self, app, nworkers, **kwargs ):
        self.step_delay = int(kwargs.get( 'step_delay', 1 ))
        super( AuroraJobRunner, self ).__init__( app, nworkers, **kwargs )
        self._init_monitor_thread()
        self._init_worker_threads()

    def queue_job( self, job_wrapper ):
        if not self.prepare_job( job_wrapper, include_metadata=True ):
            return

        # container config
        if job_wrapper.tool.containers:
            container_image = job_wrapper.tool.containers[0].identifier
        else:
            log.warning("container not found")
            container_image = DEFAULT_AURORA_CONTAINER
        log.debug("container image = %s", container_image)

        # job annotation
        param_dict = job_wrapper.get_param_dict()
        #for k,v in param_dict.items():
        #    log.debug("params key=%s value=%s", k, v)
        if param_dict.has_key('job_annotation'):
            job_annotation = param_dict['job_annotation']
        else:
            job_annotation = 'none'
        log.debug("job annotation=%s", job_annotation)

        ajs = AuroraJobState(
            files_dir=job_wrapper.working_directory,
            job_wrapper=job_wrapper,
        )
        ajs.old_state = 'new'
        ajs.job_id = job_wrapper.get_id_tag()
        ajs.container = container_image
        ajs.job_annotation = job_annotation

        job_script_props = {
            'exit_code_path': ajs.exit_code_file,
        }
        script = self.get_job_file( job_wrapper, **job_script_props)

        log.debug("job file=%s", ajs.job_file)
        log.debug("output file=%s", ajs.output_file)
        log.debug("error file=%s", ajs.error_file)
        log.debug("exit_code_file=%s", ajs.exit_code_file)
        log.debug("ajs job_id=%s", ajs.job_id)
        log.debug("ajs job_name=%s", ajs.job_name)

        try:
            fh = file( ajs.job_file, "w" )
            fh.write( self.create_post_process(script) )
            fh.close()
        except:
            log.exception( "(%s) failure preparing job script" % ajs.job_id )
            job_wrapper.fail( "failure preparing job script", exception=True )
            return

        # job was deleted while we were preparing it
        if job_wrapper.get_state() == model.Job.states.DELETED:
            log.debug( "Job %s deleted by user before it entered the queue" % ajs.job_id )
            if self.app.config.cleanup_job in ( "always", "onsuccess" ):
                job_wrapper.cleanup()
            return

        self.submit_job(ajs)

        # Add to our 'queue' of jobs to monitor
        self.monitor_queue.put( ajs )

    def check_watched_items( self ):
        #time.sleep( self.step_delay )
        new_watched = []
        for ajs in self.watched:
            #log.debug( '(%s) State check, last state was: %s',
            #           ajs.job_id, ajs.old_state )

            if ajs.old_state == model.Job.states.NEW:
                log.debug( '(%s) Job state changed to queued', ajs.job_id )
                ajs.job_wrapper.change_state( model.Job.states.QUEUED )
                ajs.old_state = model.Job.states.QUEUED

            elif ajs.old_state == model.Job.states.QUEUED:
                log.debug( '(%s) current Job state is queued', ajs.job_id )
                state = self.get_job_status(ajs)
                if state == model.Job.states.RUNNING:
                    ajs.running = True
                    ajs.job_wrapper.change_state( model.Job.states.RUNNING )
                    ajs.old_state = model.Job.states.RUNNING
                elif state == model.Job.states.ERROR:
                    log.debug( '(%s) Job failed', ajs.job_id )
                    ajs.failed = True
                    self.work_queue.put( ( self.finish_job, ajs ) )
                    continue
                elif state == model.Job.states.OK:
                    log.debug( '(%s) Job has completed', ajs.job_id )
                    self.work_queue.put( ( self.finish_job, ajs ) )
                    continue

            elif ajs.old_state == model.Job.states.RUNNING:
                #log.debug( '(%s) current Job state is running', ajs.job_id )
                state = self.get_job_status(ajs)
                if state == model.Job.states.OK:
                    log.debug( '(%s) Job has completed', ajs.job_id )
                    self.work_queue.put( ( self.finish_job, ajs ) )
                    continue
                elif state == model.Job.states.ERROR:
                    log.debug( '(%s) Job failed', ajs.job_id )
                    ajs.failed = True
                    self.work_queue.put( ( self.finish_job, ajs ) )
                    continue

            new_watched.append( ajs )

        self.watched = new_watched

    def stop_job( self, job ):
        raise NotImplementedError()

    def recover( self, job, job_wrapper ):
        raise NotImplementedError()

    ###########################################################################

    def create_post_process( self, script ):
        cmdline = ""
        for cmd in script.split('; '):
            if 0 < cmd.count('docker ') or 0 < cmd.count('set_metadata'):
                continue
            cmdline += cmd + '\n'
        log.debug('cmdline=%s', cmdline)
        return cmdline

    def get_job_status( self, ajs ):
        statuses = ajs.aurora_job_statuses
        finished = 0
        running = 0
        for job_id in statuses.keys():
            templ = string.Template(AURORA_JOB_CMD_TEMPL)
            cmd = templ.substitute(command="status", id=job_id, option='')
            #log.debug("aurora status command=%s", cmd)
            proc = subprocess.Popen(args=cmd,
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    env=os.environ.copy(),
                                    preexec_fn=os.setpgrp)
            stdout_data, stderr_data = proc.communicate()
            #log.debug("aurora job create stdout=%s", stdout_data)
            #log.debug("aurora job create stderr=%s", stderr_data)

            if 0 < stdout_data.count('status: FINISHED'):
                finished += 1
            elif 0 < stdout_data.count('status: RUNNING'):
                running += 1
                break
            elif 0 < stdout_data.count('status: FAILED'):
                log.error("aurora status (%s) failed: %s", ajs.job_id, job_id)
                return model.Job.states.ERROR

        if 0 < running:
            log.debug("aurora status (%s) is running. (finished=%d)", ajs.job_id, finished)
            return model.Job.states.RUNNING
        elif len(statuses) == finished:
            log.debug("aurora status (%s) is finished.", ajs.job_id)

            # Merge BAM files (tophat_out.XXX/*.bam)
            if ajs.job_annotation == 'tophat.parallel':
                cmd = "cd %s;" % ajs.job_wrapper.working_directory
                cmd += "mkdir -p tophat_out;samtools merge tophat_out/accepted_hits.bam tophat_out.*/accepted_hits.bam"
                proc = subprocess.Popen(args=cmd,
                                        shell=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        env=os.environ.copy(),
                                        preexec_fn=os.setpgrp)
                out, err = proc.communicate()
                log.debug("Merge BAMs stdout=%s", out)
                log.debug("Merge BAMs stderr=%s", err)

            # execute post-process
            script = "/bin/sh " + ajs.job_file
            log.debug("post-process script=%s", script)
            proc = subprocess.Popen(args=script,
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    env=os.environ.copy(),
                                    preexec_fn=os.setpgrp)
            out, err = proc.communicate()
            log.debug("post-process stdout=%s", out)
            log.debug("post-process stderr=%s", err)
#            for job_id in statuses.keys():
#               self.detail_job_status(ajs, job_id)
            return model.Job.states.OK
        else:
            return model.Job.states.QUEUED

    def submit_job( self, ajs ):
        output_path = ajs.job_wrapper.get_output_fnames()[0]
        log.debug('output_path=%s', output_path)

        # set working directory on container
        tool_script = open("%s/tool_script.sh" % ajs.job_wrapper.working_directory, 'r').read()
        chdir_cmd = "#!/bin/sh\ncd %s;" % ajs.job_wrapper.working_directory
        tool_script = tool_script.replace("#!/bin/sh\n", chdir_cmd)

        # generate aurora configuration
        aurora_job_commands = {}
        if ajs.job_annotation == 'split':
            templ1 = string.Template(SPLIT_CMD_TEMPL)
            cmd = tool_script + templ1.substitute(datafile=output_path)

            templ2 = string.Template(AURORA_JOB_CONF_TEMPL)
            aurora_conf = templ2.substitute(id=ajs.job_id,
                                            cmdline=cmd,
                                            container=ajs.container,
                                            cpu=8, ram_gb=4, disk_gb=4)
            confpath = "/tmp/galaxyjob%s.aurora" % ajs.job_id
            open(confpath, 'w').write(aurora_conf)

            templ3 = string.Template(AURORA_JOB_CMD_TEMPL)
            aurora_cmd = templ3.substitute(command="create",
                                           id=ajs.job_id,
                                           option=confpath)
            aurora_job_commands[ajs.job_id] = aurora_cmd

        elif ajs.job_annotation == 'tophat.parallel':
            input_path1 = ajs.job_wrapper.get_input_fnames()[0] # forward reads
            input_path2 = ajs.job_wrapper.get_input_fnames()[1] # reverse reads
            log.debug('tophat.parallell input-paired:%s, %s', input_path1, input_path2)

            # split fastq
            self.split_fastq(input_path1, TOPHAT_DEFAULT_PARALLEL_NUM)
            self.split_fastq(input_path2, TOPHAT_DEFAULT_PARALLEL_NUM)

            fw_files = sorted(glob.glob('%s.???' % input_path1))
            rev_files = sorted(glob.glob('%s.???' % input_path2))
            for (forward, reverse) in zip(fw_files, rev_files):
                log.debug('forward=%s, reverse=%s', forward, reverse)
                fw_splitext = os.path.splitext(forward)
                rev_splitext = os.path.splitext(reverse)
                cmd = tool_script.replace(fw_splitext[0], forward)
                cmd = cmd.replace(rev_splitext[0], reverse)

                # replace output-dir: ./tophat_out.XXX
                # *** MUST ***  tophat command options "--output-dir"
                cmd = cmd.replace('--output-dir ./tophat_out',
                                  '--output-dir ./tophat_out%s' % fw_splitext[1])
                log.debug('tophat.parallel tool_script=\n%s', cmd)

                templ1 = string.Template(AURORA_JOB_CONF_TEMPL)
                job_id = "%s_%s" % (ajs.job_id, forward[len(forward)-3:])
                aurora_conf = templ1.substitute(id=job_id,
                                                cmdline=cmd,
                                                container=ajs.container,
                                                cpu=1, ram_gb=3, disk_gb=4)
                confpath = "/tmp/galaxyjob%s.aurora" % job_id
                open(confpath, 'w').write(aurora_conf)

                templ2 = string.Template(AURORA_JOB_CMD_TEMPL)
                aurora_cmd = templ2.substitute(command="create",
                                               id=job_id,
                                               option=confpath)
                aurora_job_commands[job_id] = aurora_cmd

        elif ajs.job_annotation == 'parallel':
            input_path = ajs.job_wrapper.get_input_fnames()[0]
            input_files = glob.glob('%s.???' % input_path)
            for infile in input_files:
                infile_splitext = os.path.splitext(infile)
                cmd = tool_script.replace(infile_splitext[0], infile) 
                outfile = str(output_path) + infile_splitext[1]
                cmd = cmd.replace(str(output_path), outfile)

                templ1 = string.Template(AURORA_JOB_CONF_TEMPL)
                job_id = "%s_%s" % (ajs.job_id, infile[len(infile)-3:])
                aurora_conf = templ1.substitute(id=job_id,
                                                cmdline=cmd,
                                                container=ajs.container,
                                                cpu=1, ram_gb=3, disk_gb=4)
                confpath = "/tmp/galaxyjob%s.aurora" % job_id
                open(confpath, 'w').write(aurora_conf)

                templ2 = string.Template(AURORA_JOB_CMD_TEMPL)
                aurora_cmd = templ2.substitute(command="create",
                                               id=job_id,
                                               option=confpath)
                aurora_job_commands[job_id] = aurora_cmd

        elif ajs.job_annotation == 'merge':
            input_path = ajs.job_wrapper.get_input_fnames()[0]
            cmd = tool_script.replace(str(input_path), input_path + '.*')
            templ1 = string.Template(AURORA_JOB_CONF_TEMPL)
            aurora_conf = templ1.substitute(id=ajs.job_id,
                                            cmdline=cmd,
                                            container=ajs.container,
                                            cpu=4, ram_gb=8, disk_gb=4)
            confpath = "/tmp/galaxyjob%s.aurora" % ajs.job_id
            open(confpath, 'w').write(aurora_conf)

            templ2 = string.Template(AURORA_JOB_CMD_TEMPL)
            aurora_cmd = templ2.substitute(command="create",
                                           id=ajs.job_id,
                                           option=confpath)
            aurora_job_commands[ajs.job_id] = aurora_cmd

        else:
            templ1 = string.Template(AURORA_JOB_CONF_TEMPL)
            aurora_conf = templ1.substitute(id=ajs.job_id,
                                            cmdline=tool_script,
                                            container=ajs.container,
                                            cpu=0.1, ram_gb=0.1, disk_gb=0.1)
            confpath = "/tmp/galaxyjob%s.aurora" % ajs.job_id
            open(confpath, 'w').write(aurora_conf)

            log.debug(aurora_conf);

            templ2 = string.Template(AURORA_JOB_CMD_TEMPL)
            aurora_cmd = templ2.substitute(command="create",
                                           id=ajs.job_id,
                                           option=confpath)
            aurora_job_commands[ajs.job_id] = aurora_cmd


        # submit aurora job
        proc_output = ''
        for job_id, job_command in aurora_job_commands.items():
            log.debug("aurora job command=%s", job_command)
            proc = subprocess.Popen(args=job_command,
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    env=os.environ.copy(),
                                    preexec_fn=os.setpgrp)
            stdout_data, stderr_data = proc.communicate()
            #log.debug("aurora job create stdout=%s", stdout_data)
            #log.debug("aurora job create stderr=%s", stderr_data)

            ajs.aurora_job_statuses[job_id] = 'created'
            proc_output += stdout_data
            proc_output += stderr_data

        # for debug
        for k,v in ajs.aurora_job_statuses.items():
            log.debug("job_id=%s, status=%s", k, v)

        # job result in galaxy
        open(ajs.exit_code_file, 'w').write('0')
        open(ajs.output_file, 'w').write(proc_output)
        open(ajs.error_file, 'w').write('')

    def detail_job_status( self, ajs, job_id ):
        templ = string.Template(AURORA_JOB_CMD_TEMPL)
        cmd = templ.substitute(command="status --write-json", id=job_id, option='')
        proc = subprocess.Popen(args=cmd,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=os.environ.copy(),
                                preexec_fn=os.setpgrp)
        stdout_data, stderr_data = proc.communicate()

        json_data = json.loads(stdout_data)
        fin_job = json_data[0]["inactive"][0]

        task = fin_job["assignedTask"]["task"]
        job = task["job"]
        job_key = "%s/%s/%s" % (job["role"], job["environment"], job["name"])
        docker_image = task["container"]["docker"]["image"]
        num_cpus = task["numCpus"]
        ram_mb = task["ramMb"]
        disk_mb = task["diskMb"]
        slave_host = fin_job["assignedTask"]["slaveHost"]

        events = fin_job["taskEvents"]
        for e in events:
            if e["status"] == "STARTING":
                starting_time = e["timestamp"]
            elif e["status"] == "FINISHED":
                finished_time = e["timestamp"]

        galaxy_job_id = job_id.split("_")[0]
        if 0 < job_id.count("_"):
            fragment_no = job_id.split("_")[1]
        else:
            fragment_no = None

        log.info("JOB STATUS: job_key=%s job_id=%s fragment_no=%s num_cpus=%s ram_mb=%s disk_mb=%s slave_host=%s docker_image=%s starting_time=%s finished_time=%s",
                  job_key, galaxy_job_id, fragment_no, num_cpus, ram_mb, disk_mb, slave_host, docker_image, starting_time, finished_time)

        # insert aurora_job
        aurora_job = AuroraJobTable(
            job_key = job_key,
            job_id = int(galaxy_job_id),
            fragment_no = int(fragment_no) if fragment_no else None,
            num_cpus = float(num_cpus),
            ram_mb = int(ram_mb),
            disk_mb = int(disk_mb),
            slave_host = slave_host,
            docker_image = docker_image,
            starting_time = int(starting_time),
            finished_time = int(finished_time),
        )
        self.sa_session.add(aurora_job)
        #self.sa_session.commit()
        self.sa_session.flush()

    def split_fastq( self, filepath, nparts ):
        cmd = "fastq-splitter.pl --n-parts %d %s" % (nparts, filepath)
        log.debug("split_fastq cmd=%s", cmd)
        proc = subprocess.Popen(args=cmd,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=os.environ.copy(),
                                preexec_fn=os.setpgrp)
        out, err = proc.communicate()
        log.debug("split_fastq stdout=%s", out)
        log.debug("split_fastq stderr=%s", err)
