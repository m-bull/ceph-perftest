import json
import matplotlib.pyplot as plt
from pathlib import Path, PurePath
import sys
from binary import BinaryUnits, DecimalUnits, convert_units

PLOTS = [
        {
            "title": "Multiple Clients\nAggregate IOPS\nmode: {mode}, BS: {bs}\nclient threads: {threads}\nMax BW (MB/s): {max_bw}, Max IOPS: {max_iops}",
            "varname": "iops",
            "y_label": "IOPS",
            "name": "iops"
            },
        {
            "title": "Multiple Clients\nAggregate Bandwidth\nmode: {mode}, BS: {bs}\nclient threads: {threads}\nMax BW (MB/s): {max_bw}, Max IOPS: {max_iops}",
            "varname": "bw",
            "y_label": "Bandwidth (MB/s)",
            "name" : "bandwidth"
	}
        ]

RW_LOOKUP = {
            'randwrite': 'write',
            'randread' : 'read',
            'read': 'read',
            'write' : 'write'
        }

GLOBAL_CONFIG = """
    [global]
    bs={bs}
    iodepth={iodepth}
    direct={direct}
    ioengine={ioengine}
    time_based
    group_reporting
    runtime={runtime}
    numjobs={numjobs}
    rw={mode}
    size={size}
    directory={directory}
    nrfiles={nrfiles}
    [{jobname}]
    """

def generate_fio_configs(bs, iodepth, direct, ioengine, runtime, numjobs, rw, size, nrfiles, directory):
    fio_configs = []
    for blocksize in bs:
        for mode in rw:
            fio_configs.append(
                {
                    "bs": blocksize,
                    "iodepth": iodepth,
                    "direct": direct,
                    "ioengine": ioengine,
                    "directory": directory,
                    "runtime": runtime,
                    "numjobs": numjobs,
                    "size": size,
                    "nrfiles": nrfiles,
                    "rw": mode
                }
            )
    return fio_configs

def slurp_fio_output(fio_output_json):   
    results_summary = []
    all_hosts = []
    for jsonfile in fio_output_json:
        f = open(jsonfile, "r")
        data = json.load(f)
        clients = [ i for i in data['client_stats'] if i['jobname'] != "All clients" ]
        
        rw = RW_LOOKUP.get(data['global options']['rw'])
        if rw == None:
            print("Unsupported rw mode")
            sys.exit(1)

        n_clients = len(clients)
        for client in clients:
                bw, _ = convert_units(
                    int(client[rw]['bw']),
                    unit=BinaryUnits.KB, 
                    to=DecimalUnits.MB
                    )
                results_summary.append(
                    {
                        'count': n_clients,
                        'hostname': client['hostname'],
                        'bw': bw,
                        'iops': int(client[rw]['iops']),
                        'numjobs': data['global options']['numjobs'],
                        'bs': data['global options']['bs'],
                        'rw': data['global options']['rw']
                    }
                )
                if client['hostname'] not in all_hosts:
                    all_hosts.append(client['hostname'])
    for count in range(1, 1+len(all_hosts)):
        count_hosts = [i['hostname'] for i in results_summary if i['count'] == count]
        for host in all_hosts:
            if host not in count_hosts:
                results_summary.append(
                    {
                        'count': count,
                        'hostname': host,
                        'bw': 0,
                        'iops': 0,
                        'numjobs': data['global options']['numjobs'],
                        'bs': data['global options']['bs'],
                        'rw': data['global options']['rw']
                    }
                )

    return sorted(results_summary, key=lambda k: (k['count'], k['hostname']))

def plot_bar(summary, outdir, output_file_prefix, plot):
    all_hosts = set([i['hostname'] for i in summary])
    labels = [str(i) for i in range(1, len(all_hosts) + 1)]
    fig, ax = plt.subplots()
    cum_size = [0] * len(all_hosts)
    
    for host in all_hosts:
        values = [i[plot['varname']] for i in summary if i['hostname'] == host]
        ax.bar(labels, values, bottom=cum_size, width=0.9, label=host)

        for a in range(len(cum_size)):
            cum_size[a] += values[a]

    client_threads = summary[0]['numjobs']
    bs = summary[0]['bs']
    rw = summary[0]['rw']

    max_perf = {'clients': 0, 'bw': 0, 'iops': 0}
    for k in range(1, len(all_hosts)):
        total_bw = sum([int(i['bw']) for i in summary if i['count'] == k])
        total_iops = sum([int(i['iops']) for i in summary if i['count'] == k])
        if total_bw > max_perf['bw']:
            max_perf['bw'] = total_bw
            max_perf['clients'] = k
            max_perf['iops'] = total_iops

    ax.tick_params(axis='x', which='major', labelsize=4)
    ax.set_title(plot['title'].format(
        bs=bs,
        mode=rw,
        threads=client_threads,
        max_bw=max_perf['bw'],
        max_iops=max_perf['iops']
        )
    )
    ax.set_ylabel(plot['y_label'])
    ax.set_xlabel('Clients active')
    ax.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    fig.savefig(
        PurePath(
            outdir
            ).joinpath(
            '{prefix}-aggregate-{mode}-{bs}-{name}.png'.format(
                prefix=output_file_prefix,
                mode=rw,
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

def run_aggregate_performance(fio_output_json, outdir, output_file_prefix):
    outdir = make_output_directory(outdir)
    summary = slurp_fio_output(fio_output_json)
    for plot in PLOTS:
     	plot_bar(summary, outdir, output_file_prefix, plot)
