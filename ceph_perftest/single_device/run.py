from binary import BinaryUnits, DecimalUnits, convert_units
from pathlib import Path, PurePath
import json
import os
import shlex
import socket
import sys
import subprocess
import matplotlib.pyplot as plt

GLOBAL_CONFIG = [
        '[global]',
        'bs={bs}',
        'direct=1',
        'ioengine=libaio',
        'time_based',
        'runtime={runtime}',
        'filesize={filesize}'
        ]


DEVICE_CONFIG = [
        "[{device_name}]",
        "rw={mode}",
        "filename={device}",
        "name=single-disk-write-{device_name}"
        ]

PLOTS = [
        {
            "title": "Single Disk, Multiple Jobs\nIOPS\nMode: {mode},BS: {bs} | Device: {device} | Host: {hostname}",
            "varname": "iops",
            "y_label": "IOPS",
            "name": "iops"
            },
        {
            "title": "Single Disk, Multiple Jobs\nBandwidth\nMode: {mode},BS: {bs} | Device: {device} | Host: {hostname}",
            "varname": "bw",
            "y_label": "Bandwidth (MB/s)",
            "name" : "bandwidth"
	    }
    ]

def run_fio(fio_exe, device, max_numjobs, cleanup, outdir, bs, mode, runtime, filesize):
    summary_output = []
    device_name = os.path.basename(device)
    for numjobs in range(1, max_numjobs + 1):
        config_fn = PurePath(
            outdir
            ).joinpath(
                "{hostname}-single-{mode}-{bs}-{numjobs}.fio".format(
                    hostname=socket.gethostname(),
                    mode=mode,
                    bs=bs,
                    numjobs=numjobs
                    )
                )
        config = []
        for line in DEVICE_CONFIG:
            config.append(
                line.format(
                    mode=mode,
                    device_name=device_name,
                    device=device
                    )
                )
            with open(config_fn, "w") as f:
                for line in GLOBAL_CONFIG:
                    line = line.format(
                        bs=bs,
                        runtime=runtime,
                        filesize=filesize
                    )
                    f.write(line+'\n')
                for line in config:
                    f.write(line+'\n')

        fio_cmd = "{fio_exe} --numjobs={numjobs} --output={config_fn}.output.json {config_fn} --output-format=json+".format(
                fio_exe=fio_exe,
                numjobs=numjobs,
                config_fn=config_fn
            )
        print("Running fio...")
        print(fio_cmd)
        subprocess.run(
            shlex.split(
                fio_cmd
                ), 
                stderr = subprocess.DEVNULL, stdout = subprocess.DEVNULL
            )
        
        f = open("{config_fn}.output.json".format(
            config_fn=config_fn
            )
        )
        
        data = json.load(f)
        _mode = mode
        if _mode == 'randread':
            _mode = 'read'
        jobs = [i[mode] for i in data['jobs']]

        if cleanup:
            Path(config_fn).unlink()
            Path(
                "{config_fn}.output.json".format(
                    config_fn=config_fn
                    )
                ).unlink()

        for job_idx in range(1, max_numjobs + 1):
            # job_idx is one-based for display purposes
            if job_idx <= numjobs:                
                bw, _ = convert_units(
                        # Back to zero-based to get the list element
                        jobs[job_idx - 1]['bw'], 
                        unit=BinaryUnits.KB, 
                        to=DecimalUnits.MB
                        )
                summary_output.append(
                    {
                        'count': numjobs,
                        'job': job_idx,
                        'bw' : bw,
                        # Back to zero-based to get the list element
                        'iops': jobs[job_idx - 1]['iops']
                    }
                )
            else:
                summary_output.append(
                    {
                        'count': numjobs,
                        'job': job_idx,
                        'bw' : 0,
                        'iops': 0
                    }
                )
    
    return summary_output
        
def plot_bar(plot, summary, device, max_numjobs, outdir, bs, mode):
    print("Making {name} plot".format(name=plot['name']))
    device_name = os.path.basename(device)
    labels = [ str(i) for i in range(1, max_numjobs + 1) ]
    fig, ax = plt.subplots()
    cum_size = [0] * max_numjobs
    for numjobs in range(1, max_numjobs + 1):
        values = [i[plot['varname']] for i in summary if i['job'] == numjobs]
        ax.bar(labels, values, bottom=cum_size, width=0.9, label=numjobs)

        for a,b in enumerate(cum_size):
            cum_size[a] += values[a]

    ax.tick_params(axis='x', which='major', labelsize=4)
    ax.set_title(plot['title'].format(
        device=device,
        bs=bs,
        hostname=socket.gethostname(),
        mode=mode
        )
    )
    ax.set_ylabel(plot['y_label'])
    ax.set_xlabel('Jobs active')
    ax.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    fig.savefig(
        PurePath(
            outdir
            ).joinpath(
            '{hostname}-single-disk-{device_name}-{mode}-{bs}-{name}.png'.format(
                hostname=socket.gethostname(),
                device_name=device_name,
                mode=mode,
                name=plot['name'],
                bs=bs
                )
            ), 
            dpi=1000, 
            bbox_inches='tight'
        )
    
def make_output_directory(outdir):
    p = Path(outdir).resolve()

    p.mkdir(parents=True, exist_ok=True)

    return p

def check_block_devices(device):
    if not Path(device).is_block_device():
        sys.exit("{device} is not a block device, quitting".format(
            device=device
            )
        )

def run_single_device(fio_exe, device, max_numjobs, cleanup, 
                      outdir, bs, mode, runtime, filesize):
    check_block_devices(device)
    outdir = make_output_directory(outdir)
    summary = run_fio(fio_exe, device, max_numjobs, cleanup, 
                      outdir, bs, mode, runtime, filesize)
    for plot in PLOTS:
   	    plot_bar(plot, summary, device, max_numjobs, outdir, bs, mode)
