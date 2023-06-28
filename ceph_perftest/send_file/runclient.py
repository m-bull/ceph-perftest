from pathlib import Path, PurePath
import json
import os
import shlex
import socket
import sys
import subprocess
import time
import matplotlib.pyplot as plt

PLOTS = [
        {
            "title": "Mutiple disks\nAggregate Bandwidth\nClient: {hostname} | Server: {iperf_server}",
            "varname": "bw",
            "y_label": "Bandwidth (MB/s)",
            "name" : "bandwidth"
	    }
    ]

def run_iperf(iperf_exe, iperf_server, devices, port_start, cleanup, outdir, runtime): 
    summary_output = []
    total_disks = len(devices)
    for idx in range(1, 1 + total_disks):
        devs_to_test = devices[:idx]
        for dev_idx,device in enumerate(devs_to_test):
            device_name = os.path.basename(device)
            output_fn = PurePath(
                outdir
                ).joinpath('iperf-out-{device_name}-{idx}.json'.format(
                    device_name=device_name,
                    idx=idx
                    )
                )
            # iperf3 -c $target -p $((5201 + $j)) -F /dev/$hdd -Z -T $hdd -J > iperf3-$count-$hdd.json &
            iperf_client_cmd = "{iperf_exe} -c {iperf_server} -p {iperf_server_port} -t {time} -F {device} -Z -T {device_name} -J".format(
                iperf_exe = iperf_exe,
                iperf_server = iperf_server,
                iperf_server_port = port_start + dev_idx,
                time = runtime,
                device = device,
                device_name = device_name
                )
            print(iperf_client_cmd)
            with open(output_fn, 'w') as f:
                cmd = shlex.split(iperf_client_cmd)
                subprocess.Popen(cmd, stdout=f)
        # This is a hack to ensure that our processes have 
        # completed before moving onto the next tranche
        time.sleep(runtime + 0.5 * len(devs_to_test))

    for idx in range(1, 1 + total_disks):
        devs_to_test = devices[:idx]
        for dev_idx,device in enumerate(devs_to_test):
            device_name = os.path.basename(device)
            output_fn = PurePath(
                outdir
                ).joinpath('iperf-out-{device_name}-{idx}.json'.format(
                    device_name=device_name,
                    idx=idx
                    )
                )
            print("analysing: {output_fn}".format(output_fn=output_fn))
            f = open(output_fn, 'r')
            data = json.load(f)

            bw = data['end']['sum_sent']['bits_per_second']

            summary_output.append(
                {
                    'count': idx,
                    'device': device,
                    'bw': bw / 8000000
                }
            )
            if cleanup:
                Path(output_fn).unlink()

        for device in devices:
            if device not in devs_to_test:
                summary_output.append(
                {
                    'count': idx,
                    'device': device,
                    'bw': 0
                }
            )

    return summary_output
        
def plot_bar(summary, plot, iperf_server, devices, port_start, cleanup, outdir, runtime, network_line_rate):
    print("Making {name} plot".format(name=plot['name']))
    labels = [ str(i) for i in range(1, len(devices) + 1) ]
    fig, ax = plt.subplots()
    cum_size = [0] * len(devices)
    for device in devices:
        values = [i[plot['varname']] for i in summary if i['device'] == device]
        ax.bar(labels, values, bottom=cum_size, width=0.9, label=device)

        for a,b in enumerate(cum_size):
            cum_size[a] += values[a]

    if network_line_rate:
        lr_mb = network_line_rate * 125
        ax.plot(
            labels,
            [lr_mb]*len(devices), 
            label="Network Line Rate ({network_line_rate} Gbps)".format(
                network_line_rate=network_line_rate
                )
            )
    ax.tick_params(axis='x', which='major', labelsize=4)
    ax.set_title(plot['title'].format(
        hostname=socket.gethostname(),
        iperf_server=iperf_server
        )
    )
    ax.set_ylabel(plot['y_label'])
    ax.set_xlabel('OSDs active')
    ax.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    fig.savefig(
        PurePath(
            outdir
            ).joinpath(
            '{hostname}-aggregate-network-{name}.png'.format(
                hostname=socket.gethostname(),
                name=plot['name'],
                )
            ), 
            dpi=1000, 
            bbox_inches='tight'
        )
    
def make_output_directory(outdir):
    p = Path(outdir).resolve()

    p.mkdir(parents=True, exist_ok=True)

    return p

def check_block_devices(devices):
    for device in devices:
        if not Path(device).is_block_device():
            sys.exit("{device} is not a block device, quitting".format(
                device=device)
                )

def run_sendfile_client(iperf_exe, iperf_server, devices, port_start, cleanup, outdir, runtime, network_line_rate):
    check_block_devices(devices)
    outdir = make_output_directory(outdir)
    summary = run_iperf(iperf_exe, iperf_server, devices, port_start, cleanup, outdir, runtime)
    for plot in PLOTS:
   	    plot_bar(summary, plot, iperf_server, devices, port_start, cleanup, outdir, runtime, network_line_rate)
