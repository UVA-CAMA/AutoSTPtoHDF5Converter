import configargparse

parser = configargparse.ArgParser(default_config_files='default.conf')
parser.add_argument('-conf', '--my-config', is_config_file=True, help='config file path')
parser.add_argument('-i', '--intput', help='Path to input files. ', type=str)
parser.add_argument('-o', '--output', help='Path to output folder. ', type=str)
parser.add_argument('-s', '--system', help='Specify system that you are using. Default: Carescape. (u = Unity, '
                                           'p = Philips Classic, cs = Carescape, pix = Philips PIICiX)', type=str,
                    choices=['u', 'p', 'cs', 'pix'], default='cs')
parser.add_argument('-w', '--wave_data', help='Include wavedata. Default: True', action='store_false')
parser.add_argument('-s', '--offset', help='Seconds since 01/01/1970 for negative offset', type=str)
parser.add_argument('-d', '--delete_stp', help='Delete STP files if conversion if successful. Default: False',
                    action='store_true')
parser.add_argument('-c', '--cores', help='Number of cores to use. Default: 6. ', type=int, default=6)
parser.add_argument('-t', '--timeout', help='Number of hours to run conversions before timeout. Default: 10', type=int,
                    default=10)

args = parser.parse_args()

if __name__ == '__main__':
    if not args.input:
        args.input = input('Path to the input folder where I should check for .stp files: ')
    if not args.output:
        args.input = input('Path to the output folder where I should put completed .hdf5 files: ')
    args.timeout = (args.timeout * 60 * 60)

    