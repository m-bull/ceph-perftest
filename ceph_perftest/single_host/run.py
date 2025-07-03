import json
import matplotlib.pyplot as plt
import os
from pathlib import Path, PurePath
import shlex
import socket
import subprocess
import sys
from binary import BinaryUnits, DecimalUnits, convert_units

GLOBAL_CONFIG = [
        '[global]',
        'bs={bs}',
        'iodepth=16',
        'direct=1',
        'ioengine=libaio',
        'randrepeat=0',
        'time_based',
        'runtime={runtime}',
        'filesize={filesize}'
        ]


DEVICE_CONFIG = [
        "[{device_name}]",
        "rw={mode}",
        "filename={device}",
        "name={device_name}"
        ]

PLOTS = [
        {
            "title": "Multiple Disks\nAggregate IOPS\nmode: {mode},BS: {bs} | {hostname}",
            "varname": "iops",
            "y_label": "IOPS",
            "name": "iops"
            },
        {
            "title": "Mutiple disks\nAggregate Bandwidth\nmode: {mode},BS: {bs} | {hostname}",
            "varname": "bw",
            "y_label": "Bandwidth (MB/s)",
            "name" : "bandwidth"
	}
        ]

def check_block_devices(devices):
    for device in devices:
        if not Path(device).is_block_device():
            sys.exit("{device} is not a block device, quitting".format(
                device=device)
                )

def run_fio(fio_exe, input_devices, cleanup, outdir, bs, mode, runtime, filesize):
    summary_output = []
    total_disks = len(input_devices)
    for idx in range(1, 1 + total_disks):
        count_output = {}
        devices = input_devices[:idx]
        config_fn = PurePath(
            outdir
            ).joinpath(
            "{hostname}-aggregate-{mode}-{bs}-{idx}.fio".format(
                hostname=socket.gethostname(),
                mode=mode,
                bs=bs,
                idx=idx
            )
        )
        config = []

        print("Device count: {count}\nDevices included: {devices}".format(
            count=idx,
            devices=",".join(devices)
            )
        )
        for device in devices:
            device_name = os.path.basename(device)
            for line in DEVICE_CONFIG:
                config.append(
                        line.format(
                            device=device,
                            idx=idx,
                            device_name=device_name,
                            mode=mode
                            )
                        )
        with open(config_fn, "w") as f:
            for line in GLOBAL_CONFIG:
                line = line.format(
                        bs=bs,
                        runtime=runtime,
                        filesize=filesize,
                        )
                f.write(line+'\n')
            for line in config:
                f.write(line+'\n')
        
        fio_cmd = "{fio_exe} --output={config_fn}.output.json {config_fn} --output-format=json+".format(
                fio_exe=fio_exe,
                config_fn=config_fn,
                )
        print("Running fio...")
        print(fio_cmd)        
        subprocess.run(
                shlex.split(
                    fio_cmd
                    ), 
                stderr = subprocess.DEVNULL, 
                stdout = subprocess.DEVNULL
                )
        
        f = open("{config_fn}.output.json".format(
            config_fn=config_fn
            )
        )
        
        data = json.load(f)

        if cleanup:
            Path(config_fn).unlink()
            Path(
                "{config_fn}.output.json".format(
                    config_fn=config_fn
                    )
                ).unlink()

        for dev_idx,device in enumerate(devices, 1):
            device_name = os.path.basename(device)
            print("Parsing output for {device}".format(
                device=device
                )
            )
            _mode = mode
            if _mode == 'randread':
                _mode = 'read'
            elif _mode == 'randwrite':
                _mode = 'write'
            dev_data = [i[_mode] for i in data['jobs'] if i['jobname'] == device_name][0]
            count_output.update(
                    {
                        device: {
                            'bw': dev_data['bw'], 
                            'iops':  dev_data['iops']
                            }
                        }
                    )

        for device in input_devices:
            if device in count_output:
                bw, _ = convert_units(
                        count_output[device]['bw'], 
                        unit=BinaryUnits.KB, 
                        to=DecimalUnits.MB
                        )
                iops = count_output[device]['iops']

            else:
                bw = iops = 0
            
            summary_output.append(
                    {
                        'count': idx,
                        'device': device,
                        'bw': bw,
                        'iops': iops
                        }
                    )

    return summary_output

def plot_bar(summary, plot, devices, outdir, bs, mode):
    print("Making {name} plot".format(name=plot['name']))
    labels = [str(i) for i in range(1, len(devices) + 1)]
    fig, ax = plt.subplots()
    cum_size = [0] * len(devices)
    for device in devices:
        values = [i[plot['varname']] for i in summary if i['device'] == device]
        ax.bar(labels, values, bottom=cum_size, width=0.9, label=device)

        for a,_ in enumerate(cum_size):
            cum_size[a] += values[a]

    ax.tick_params(axis='x', which='major', labelsize=4)
    ax.set_title(plot['title'].format(
        bs=bs,
        hostname=socket.gethostname(),
        mode=mode
        )
    )
    ax.set_ylabel(plot['y_label'])
    ax.set_xlabel('Devices active')
    ax.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    fig.savefig(
        PurePath(
            outdir
            ).joinpath(
            '{hostname}-aggregate-{mode}-{bs}-{name}.png'.format(
                hostname=socket.gethostname(),
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

def run_single_host(fio_exe, devices, cleanup, outdir, bs, mode, runtime, filesize):
    check_block_devices(devices)
    outdir = make_output_directory(outdir)
    summary = run_fio(fio_exe, devices, cleanup, outdir, bs, mode, runtime, filesize)
    for plot in PLOTS:
    	plot_bar(summary, plot, devices, outdir, bs, mode)
