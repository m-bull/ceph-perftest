import click
from shutil import which
from sys import exit
from .mixed_io.run import run_mixed_io
from .fs_aggregate.run import run_aggregate_performance
from .single_device.run import run_single_device
from .single_host.run import run_single_host
from .send_file.runclient import run_sendfile_client
from .send_file.runserver import run_sendfile_server, stop_sendfile_server

def is_exe(exe):
    """
    Check whether exe is on PATH and marked as executable.
    """
    if which(exe) is None:
        print("This subcommand requires {exe} accessible on the PATH.".format(
            exe = exe
        ))
        exit(1)
    return which(exe)

@click.group()
def cli():
    pass

@cli.command()
@click.argument('fio_output_json',
                type=click.Path(exists=True, resolve_path=True, file_okay=True),
                nargs=-1)
@click.option('-o',
              '--outdir',
              type=str,
              default='.',
              help="Output directory for plots and fio job and json files [Default: .].")
@click.option('-p',
              '--output-file-prefix',
              type=str,
              default='',
              help="Prefix for output plots filenames [Default: ''].")
@click.option('-a',
              '--annotation',
              type=str,
              default='',
              help="Additional plot annotation [Default: ''].")
@click.pass_context
def mixed_io(ctx, fio_output_json, outdir, output_file_prefix, annotation):
    """
    Plot Mixed IO fio JSON output.

    FIO_OUTPUT_JSON: fio output JSON file. May be supplied many times.
    """
    run_mixed_io(fio_output_json, outdir, output_file_prefix, annotation)

@cli.command()
@click.argument('fio_output_json',
                type=click.Path(exists=True, resolve_path=True, file_okay=True),
                nargs=-1)
@click.option('-o',
              '--outdir',
              type=str,
              default='.',
              help="Output directory for plots and fio job and json files [Default: .].")
@click.option('-p',
              '--output-file-prefix',
              type=str,
              default='',
              help="Prefix for output plots filenames [Default: ''].")
@click.pass_context
def aggregate_performance(ctx, fio_output_json, outdir, output_file_prefix):
    """
    Plot Aggregate Performance fio JSON output.

    FIO_OUTPUT_JSON: fio output JSON file. May be supplied many times.
    """

    run_aggregate_performance(fio_output_json, outdir, output_file_prefix)

@cli.command()
@click.argument('block_device', 
                type=click.Path(exists=True, resolve_path=True))
@click.argument('max_numjobs', 
                type=int)
@click.option('-c', '--cleanup', 
              is_flag=True, 
              help="Clean up fio job and output JSON files [Default: no]." )
@click.option('-o', '--outdir', 
              type=str, 
              default='.', 
              help="Output directory for plots and fio job and json files [Default: .].")
@click.option('-b', '--bs',
              type=str,
              default='8k',
              help='fio bs parameter [Default: 8k].')
@click.option('-m', '--mode',
              type=str,
              default='write',
              help='fio rw parameter [Default: write].')
@click.option('-r', '--runtime',
              type=str,
              default='30',
              help='fio runtime parameter in seconds [Default: 30].')
@click.option('-f', '--filesize',
              type=str,
              default='2G',
              help='fio filesize parameter [Default: 2G].')
@click.pass_context
def single_device(ctx, device, max_numjobs, cleanup,
                  outdir, bs, mode, runtime, filesize):
    """
    Use fio to test a single device with multiple jobs.
    \b
    BLOCK_DEVICE: The path to the block device to test.
    MAX_NUMJOBS: Maximum number of fio jobs to test.
    """

    fio_exe = is_exe("fio")
    run_single_device(fio_exe, device, max_numjobs, cleanup, 
                          outdir, bs, mode, runtime, filesize)

@cli.command()
@click.argument('block_devices',
                type=click.Path(exists=True, resolve_path=True),
                nargs=-1)
@click.option('-c', '--cleanup',
              is_flag=True,
              help="Clean up fio job and output JSON files [Default: no]." )
@click.option('-o', '--outdir',
              type=str,
              default='.',
              help="Output directory for plots and fio job and json files [Default: .].")
@click.option('-b', '--bs',
              type=str,
              default='8k',
              help='fio bs parameter [Default: 8k].')
@click.option('-m', '--mode',
              type=str,
              default='read',
              help='fio rw parameter [Default: write].')
@click.option('-r', '--runtime',
              type=str,
              default='30',
              help='fio runtime parameter in seconds [Default: 30].')
@click.option('-f', '--filesize',
              type=str,
              default='2G',
              help='fio filesize parameter [Default: 2G].')
@click.pass_context
def single_host(ctx, block_devices, cleanup, outdir, bs, mode, runtime, filesize):
    """
    Use fio to test all devices on a host.
    """
    fio_exe = is_exe("fio")
    run_single_host(fio_exe, block_devices, cleanup, outdir, bs, mode, runtime, filesize)

@cli.group()
@click.pass_context
def send_file(ctx):
    """
    Use iperf3 to test device-to-network performance.
    """
    pass

@send_file.group()
@click.pass_context
def server(ctx):
    """
    Start and stop multiple iperf3 servers.
    """
    pass

@server.command('start')
@click.argument('server_count',
                type=int,
                default=1)
@click.option('-p', '--port_start',
              type=int,
              default=5201,
              help="First iperf3 server port (assumed consecutive after this) [Default: 5201]." )
@click.pass_context
def sendfile_server_start(ctx, server_count, port_start):
    """Start a series of iperf3 servers listening on consecutive ports.

    \b
    SERVER_COUNT: Number of iperf3 servers to start.
    """
    iperf_exe = is_exe("iperf3")
    run_sendfile_server(iperf_exe, server_count, port_start)

@server.command('stop')
@click.pass_context
def sendfile_server_start(ctx):
    """
    \b
    Stop iperf3 servers started by
    ceph-perftest sendfile server start
    """
    stop_sendfile_server()

@send_file.command('client')
@click.argument('iperf_server',
                type=str)
@click.argument('device',
                type=click.Path(exists=True, resolve_path=True),
                nargs=-1)
@click.option('-p', '--port_start',
              type=int,
              default=5201,
              help="First iperf3 server port (assumed consecutive after this) [Default: 5201]." )
@click.option('-c', '--cleanup',
              is_flag=True,
              help="Clean up fio job and output JSON files [Default: no]." )
@click.option('-o', '--outdir',
              type=str,
              default='.',
              help="Output directory for plots and iperf json files [Default: .].")
@click.option('-r', '--runtime',
              type=int,
              default='10',
              help='iperf3 client time parameter in seconds [Default: 10].')
@click.option('-n', '--network-line-rate',
              type=int,
              help="Network line rate in Gbit" )
@click.pass_context
def sendfile_client(ctx, iperf_server, device, port_start, cleanup, outdir, runtime, network_line_rate):
    """Plot the aggregated network read bandwidth of a set
    of block devices, such as Ceph OSDs, using iperf3.

    \b
    IPERF_SERVER: Machine running iperf3 in server mode. to start
    iperf3 servers on another machine use:
    ceph-perftest sendfile server start
    

    DEVICE: Path to a block device
    """
    iperf_exe = is_exe("iperf3")
    run_sendfile_client(iperf_exe, iperf_server, device, port_start, cleanup, outdir, runtime, network_line_rate)

