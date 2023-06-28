from pathlib import Path, PurePath
import os
import psutil
import shlex
import signal
import subprocess
import tempfile

PID_TEMPDIR = PurePath(
    tempfile.gettempdir()
    ).joinpath("tmp.fio_perftest")

def run_sendfile_server(iperf_exe, device_count, port_start):
    
    os.makedirs(PID_TEMPDIR, exist_ok = True)
    
    for port_idx in range(device_count):
        
        pid_tempfile = PID_TEMPDIR.joinpath("fio." + str(port_idx) + ".pid")    
        
        # iperf3 -s -p $((5200 + $i)) &
        iperf_server_cmd = "{iperf_exe} -s -D -p {iperf_server_port} -I {pidfile_path}".format(
            iperf_exe = iperf_exe,
            iperf_server_port = port_start + port_idx,
            pidfile_path = pid_tempfile
            )
    
        print(iperf_server_cmd)
    
        subprocess.Popen(
            shlex.split(
            iperf_server_cmd
            )
        )

def stop_sendfile_server():
    
    temp_pidfiles = sorted(
        Path(PID_TEMPDIR).glob('*.pid')
        )

    for pidfile in temp_pidfiles:
        with open(pidfile, 'r') as f:
            pid = int(f.read().replace('\00', ''))
            if psutil.pid_exists(pid):
                if "iperf3" in psutil.Process(pid).exe():
                    os.kill(pid, signal.SIGKILL)

        os.unlink(pidfile)
        print(pidfile)
