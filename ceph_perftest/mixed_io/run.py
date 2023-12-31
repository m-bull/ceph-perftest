from plotnine import ggplot, geom_col, aes, \
                    facet_grid, theme_bw, scale_fill_hue, \
                    ylab, xlab, labs, labeller
import json

from pathlib import Path, PurePath
import re
import pandas as pd
from binary import BinaryUnits, DecimalUnits, convert_units

def slurp_fio_output(fio_output_json):   
    results_summary = []
    for jsonfile in fio_output_json:

        f = open(jsonfile, "r")
        data = json.load(f)

        if len(data['client_stats']) == 1:
            all_clients = data['client_stats']
        else:
            all_clients = [ i for i in data['client_stats'] if i['jobname'] == 'All clients']

        for client in all_clients:
            for io_type in ['read', 'write']:
                for measure in ['iops', 'bw']:
                    bs_val, bs_unit = re.findall('[A-Za-z]+|\\d+', data['global options']['bs'])
                    if bs_unit.upper() == "M":
                        bs = int(bs_val) * 1000 * 1000
                    elif bs_unit.upper() == "K":
                        bs = int(bs_val) * 1000

                    client_data = {
                            'count': len([ i for i in data['client_stats'] if i['jobname'] != 'All clients']),
                            'random_io_pct': data['global options']['percentage_random'],
                            'read_io_pct': data['global options']['rwmixread'],
                            'bs': bs,
                            'numjobs': data['global options']['numjobs'],
                            'io_type': io_type,
                            'measure': measure
                        }
                    
                    if measure == 'bw':
                        bw, _ = convert_units(
                            int(client[io_type]['bw']),
                            unit=BinaryUnits.KB, 
                            to=DecimalUnits.MB
                            )
                        client_data['value'] = bw

                    else:
                        client_data['value'] = int(client[io_type]['iops'])

                    results_summary.append(client_data)
    
    return pd.DataFrame(results_summary)

def make_output_directory(outdir):
    p = Path(outdir).resolve()

    p.mkdir(parents=True, exist_ok=True)

    return p

def label_col_facets(s):
    return 'Random IO (%): ' + s

def label_row_facets(s):
    i = int(s)
    if i >= 1000 * 1000:
        units = "M"
        value = i / 1000 / 1000
    elif i > 1000:
        units = "K"
        value = i / 1000 
    return 'IO Blocksize: {value}{units}'.format(value=int(value), units=units)

def order_numerical_categories(s):
    return [
        str(j) for j in sorted(
            set(
                [
                    int(i) for i in s
                    ]
                )
            )
        ]

def make_plots(summary, outdir, output_file_prefix, annotation):
    summary['read_io_pct'] = pd.Categorical(
        summary['read_io_pct'], 
        categories=order_numerical_categories(summary['read_io_pct'])
        )
    summary['random_io_pct'] = pd.Categorical(
        summary['random_io_pct'], 
        categories=order_numerical_categories(summary['random_io_pct'])
        )
    
    n_clients = summary['count'].values[0]

    client_threads = summary['numjobs'].values[0]

    for measure in set(summary['measure']):
        if measure == 'bw':
            var = "bandwidth"
            y_label = "Bandwidth (MB/s)"
        else:
            var = y_label = measure
        p = (ggplot(
            summary.loc[summary['measure'] == measure], 
            aes(x='read_io_pct', y='value', fill='io_type')
            ) 
            + geom_col(stat='identity')
            + facet_grid('bs ~ random_io_pct', 
                          scales='free_y', 
                          labeller=labeller(
                            cols=label_col_facets, 
                            rows=label_row_facets)
                          )
            + scale_fill_hue(name="IO Type")
            + ylab(y_label)
            + xlab("Read IO mix (%)")
            + theme_bw()
            + labs(
                title="Filesystem aggregate " +var+ " under varying IO workloads",
                caption="Clients: {clients} | Client Threads: {client_threads} | {annotation}".format(
                    clients=n_clients,
                    client_threads=client_threads,
                    annotation=annotation
                )
            )
        )

        p.save(PurePath(
            outdir
            ).joinpath(
                '{prefix}.{measure}.mixed-io.png'.format(
                    prefix=output_file_prefix, measure=measure
                    )
                    ),
                    dpi=300, 
                    width=12, 
                    height=8
            )
def run_mixed_io(fio_output_json, outdir, output_file_prefix, annotation):
    outdir = make_output_directory(outdir)
    summary = slurp_fio_output(fio_output_json)
    print(summary)
    make_plots(summary, outdir, output_file_prefix, annotation)