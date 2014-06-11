#!/usr/bin/env python
"""
.. module:: rewrite_time_axis.py
   :platform: Unix
   :synopsis: Rewrite and/or check time axis of CMIP5 files upon local IPSL-ESGF datanode.

.. moduleauthor:: Levavasseur, G. <glipsl@ipsl.jussieu.fr> and Laliberte, F. <frederic.laliberte@utoronto.ca>


"""

# Module imports
import re, os, sys, argparse
import numpy as np
from netCDF4 import Dataset, date2num, num2date
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool



# Program version
__version__ = '{0} {1}-{2}-{3}'.format('v0.2', '2014', '06', '04')

# Throttle upon number of threads to spawn
_THREAD_POOL_SIZE = 4


def _control_directory(ctx):
    """Controls CMIP5 directory pattern.""" 
    #pattern = re.compile(r'/prodigfs/esg/CMIP5/merge/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/latest/([\w.-]+)/')
    pattern = re.compile(r'/data/glipsl/test_time_axis/([\w.-]+)/')
    if re.match(pattern, ctx.directory) == None:
        print "ERROR: {0} does not follow CMIP5 DRS.".format(ctx.directory)
        sys.exit(1)
    else:
        for file in _get_file_list(ctx):
            # Control filename pattern
            _control_file(file) # Modifier le sys.exit


def _control_file(file):
    """Controls CMIP5 file pattern.""" 
    pattern = re.compile(r'([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\d]+)-([\d]+).nc')
    if re.match(pattern, file) == None: 
        print 'ERROR: Invalid filename (%s does not follow CMIP5 DRS).' % file
        sys.exit(1)


def _get_file_list(ctx):
    """Returns sorted list of filenames in directory."""
    return sorted(os.listdir(ctx.directory))


def _time_init(ctx):
    """Returns required reference time properties as tuple."""
    file = _get_file_list(ctx)[0]
    _control_file(file)
    data = Dataset(ctx.directory+file, 'r')
    ctx.frequency = data.frequency
    ctx.realm = data.modeling_realm
    ctx.calendar = data.variables['time'].calendar
    ctx.tunits = data.variables['time'].units
    ctx.funits = _convert_time_units(ctx.tunits, ctx.frequency)
    data.close()
    if ctx.is_verbose:
        print '   |'.ljust(100)
        print '   |--> Frequency:'.ljust(40)+'{0}'.format(ctx.frequency).rjust(60)
        print '   |--> Calendar:'.ljust(40)+'{0}'.format(ctx.calendar).rjust(60)
        print '   |--> Time units:'.ljust(40)+'{0}'.format(ctx.tunits).rjust(60)
        print '   |'.ljust(100)


def _is_instant_time_axis(filename, realm) :
   """Returns True if time time axis an instant axis."""
   need_instant_time = [('tas','3hr','atmos'),('uas','3hr','atmos'),('vas','3hr','atmos'),('huss','3hr','atmos'),('mrsos','3hr','land'),('tslsi','3hr','land'),('tso','3hr','ocean'),('ps','3hr','atmos'),('ua','6hrPlev','atmos'),('va','6hrPlev','atmos'),('ta','6hrPlev','atmos'),('psl','6hrPlev','atmos'),('clcalipso','cf3hr','atmos'),('clcalipso2','cf3hr','atmos'),('cfadDbze94','cf3hr','atmos'),('cfadLidarsr532','cf3hr','atmos'),('parasolRefl','cf3hr','atmos'),('cltcalipso','cf3hr','atmos'),('cllcalipso','cf3hr','atmos'),('clmcalipso','cf3hr','atmos'),('clhcalipso','cf3hr','atmos'),('lon','cf3hr','atmos'),('lat','cf3hr','atmos'),('tas','cf3hr','atmos'),('ts','cf3hr','atmos'),('tasmin','cf3hr','atmos'),('tasmax','cf3hr','atmos'),('psl','cf3hr','atmos'),('ps','cf3hr','atmos'),('uas','cf3hr','atmos'),('vas','cf3hr','atmos'),('sfcWind','cf3hr','atmos'),('hurs','cf3hr','atmos'),('huss','cf3hr','atmos'),('pr','cf3hr','atmos'),('prsn','cf3hr','atmos'),('prc','cf3hr','atmos'),('evspsbl','cf3hr','atmos'),('sbl','cf3hr','atmos'),('tauu','cf3hr','atmos'),('tauv','cf3hr','atmos'),('hfls','cf3hr','atmos'),('hfss','cf3hr','atmos'),('rlds','cf3hr','atmos'),('rlus','cf3hr','atmos'),('rsds','cf3hr','atmos'),('rsus','cf3hr','atmos'),('rsdscs','cf3hr','atmos'),('rsuscs','cf3hr','atmos'),('rldscs','cf3hr','atmos'),('rsdt','cf3hr','atmos'),('rsut','cf3hr','atmos'),('rlut','cf3hr','atmos'),('rlutcs','cf3hr','atmos'),('rsutcs','cf3hr','atmos'),('prw','cf3hr','atmos'),('clt','cf3hr','atmos'),('clwvi','cf3hr','atmos'),('clivi','cf3hr','atmos'),('rtmt','cf3hr','atmos'),('ccb','cf3hr','atmos'),('cct','cf3hr','atmos'),('ci','cf3hr','atmos'),('sci','cf3hr','atmos'),('fco2antt','cf3hr','atmos'),('fco2fos','cf3hr','atmos'),('fco2nat','cf3hr','atmos'),('cltc','cf3hr','atmos'),('zfull','cf3hr','atmos'),('zhalf','cf3hr','atmos'),('pfull','cf3hr','atmos'),('phalf','cf3hr','atmos'),('ta','cf3hr','atmos'),('h2o','cf3hr','atmos'),('clws','cf3hr','atmos'),('clis','cf3hr','atmos'),('clwc','cf3hr','atmos'),('clic','cf3hr','atmos'),('reffclws','cf3hr','atmos'),('reffclis','cf3hr','atmos'),('reffclwc','cf3hr','atmos'),('reffclic','cf3hr','atmos'),('grpllsprof','cf3hr','atmos'),('prcprof','cf3hr','atmos'),('prlsprof','cf3hr','atmos'),('prsnc','cf3hr','atmos'),('prlsns','cf3hr','atmos'),('reffgrpls','cf3hr','atmos'),('reffrainc','cf3hr','atmos'),('reffrains','cf3hr','atmos'),('reffsnowc','cf3hr','atmos'),('reffsnows','cf3hr','atmos'),('dtaus','cf3hr','atmos'),('dtauc','cf3hr','atmos'),('dems','cf3hr','atmos'),('demc','cf3hr','atmos'),('clc','cf3hr','atmos'),('cls','cf3hr','atmos'),('tas','cfSites','atmos'),('ts','cfSites','atmos'),('psl','cfSites','atmos'),('ps','cfSites','atmos'),('uas','cfSites','atmos'),('vas','cfSites','atmos'),('sfcWind','cfSites','atmos'),('hurs','cfSites','atmos'),('huss','cfSites','atmos'),('pr','cfSites','atmos'),('prsn','cfSites','atmos'),('prc','cfSites','atmos'),('evspsbl','cfSites','atmos'),('sbl','cfSites','atmos'),('tauu','cfSites','atmos'),('tauv','cfSites','atmos'),('hfls','cfSites','atmos'),('hfss','cfSites','atmos'),('rlds','cfSites','atmos'),('rlus','cfSites','atmos'),('rsds','cfSites','atmos'),('rsus','cfSites','atmos'),('rsdscs','cfSites','atmos'),('rsuscs','cfSites','atmos'),('rldscs','cfSites','atmos'),('rsdt','cfSites','atmos'),('rsut','cfSites','atmos'),('rlut','cfSites','atmos'),('rlutcs','cfSites','atmos'),('rsutcs','cfSites','atmos'),('prw','cfSites','atmos'),('clt','cfSites','atmos'),('clwvi','cfSites','atmos'),('clivi','cfSites','atmos'),('rtmt','cfSites','atmos'),('ccb','cfSites','atmos'),('cct','cfSites','atmos'),('ci','cfSites','atmos'),('sci','cfSites','atmos'),('fco2antt','cfSites','atmos'),('fco2fos','cfSites','atmos'),('fco2nat','cfSites','atmos'),('cl','cfSites','atmos'),('clw','cfSites','atmos'),('cli','cfSites','atmos'),('mc','cfSites','atmos'),('ta','cfSites','atmos'),('ua','cfSites','atmos'),('va','cfSites','atmos'),('hus','cfSites','atmos'),('hur','cfSites','atmos'),('wap','cfSites','atmos'),('zg','cfSites','atmos'),('rlu','cfSites','atmos'),('rsu','cfSites','atmos'),('rld','cfSites','atmos'),('rsd','cfSites','atmos'),('rlucs','cfSites','atmos'),('rsucs','cfSites','atmos'),('rldcs','cfSites','atmos'),('rsdcs','cfSites','atmos'),('tnt','cfSites','atmos'),('tnta','cfSites','atmos'),('tntmp','cfSites','atmos'),('tntscpbl','cfSites','atmos'),('tntr','cfSites','atmos'),('tntc','cfSites','atmos'),('tnhus','cfSites','atmos'),('tnhusa','cfSites','atmos'),('tnhusc','cfSites','atmos'),('tnhusd','cfSites','atmos'),('tnhusscpbl','cfSites','atmos'),('tnhusmp','cfSites','atmos'),('evu','cfSites','atmos'),('edt','cfSites','atmos'),('pfull','cfSites','atmos'),('phalf','cfSites','atmos')]
   pattern = re.compile(r'([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\d]+)-([\d]+).nc')
   if (pattern.search(filename).group(1), pattern.search(filename).group(2), realm) in need_instant_time:
      return True
   else:
      return False


def _get_args():
    """Returns parsed command line arguments."""
    parser=argparse.ArgumentParser(
        description="""Rewrite and/or check CMIP5 file time axis on CICLAD filesystem, considering \n(i) uncorrupted filename period dates and \n(ii) properly-defined times units, time calendar and frequency NetCDF attributes.""",
        epilog="Developped by G. Levavasseur (CNRS/IPSL)"
       )
    parser.add_argument('directory',
                        nargs='?',
                        help='Path to browse following CMIP5 DRS to the variable facet in latest directory (e.g., /prodigfs/esg/CMIP5/merge/NCAR/CCSM4/amip/day/atmos/cfDay/r7i1p1/latest/ps/)')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--check',
                        action='store_true',
                        default=False,
                        help='Check time axis squareness.')
    group.add_argument('-w', '--write',
                        action='store_true',
                        default=False,
                        help="""Rewrite time axis depending on checking (including -c/--check). \nTHIS ACTION DEFINITELY MODIFY INPUT FILES !""")
    group.add_argument('-f', '--force',
                        action='store_true',
                        default=False,
                        help="""Force time axis writing overpassing checking step. \nTHIS ACTION DEFINITELY MODIFY INPUT FILES !""")
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose mode')
    parser.add_argument('-V', '--version',
                        action='version',
                        version="%(prog)s ({0})".format(__version__),
                        help='Program version')
#    return parser.parse_args(['/data/glipsl/test_time_axis/ta/','-c','-v'])
    return parser.parse_args()


def _convert_time_units(unit, frequency):
    """Converts file default time units depending on frequency."""
    units = {'3hr':'hours', 
             '6hr':'hours',
             'day':'days',
             'mon':'months',
             'yr' :'years'}
    return units[frequency]+" {1} {2} {3}".format(*unit.split(' '))


def _dates_from_filename(filename):
    """Returns datetime objetcs for start and end dates in filename."""
    pattern = re.compile(r'([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\d]+)-([\d]+).nc')
    dates=[]
    for i in [6, 7]:
        digits = pattern.search(filename).group(i).ljust(14,'0')
        dates.append(datetime.strptime(digits, '%Y%m%d%H%M%S'))
    return dates


def _time_checker(right_axis, test_axis) :
   """Returns True if right axis is right."""
   if np.array_equal(right_axis, test_axis) :
      return True
   else :
      return False


def _last_date_checker(last, end):
    """Returns True if last and end date are the same."""
    if last != end:
        return False
    else:
        return True


def _time_inc(frequency):
    """Return tuple used for time incrementation depending on frequency: (raising value, time unit)."""
    inc = {'subhr': 30,
           '3hr'  : 3,
           '6hr'  : 6,
           'day'  : 1,
           'mon'  : 1,
           'yr'   : 1}
    return inc[frequency]


def _rebuild_time_axis(start, length, instant, inc, ctx):
    """Rebuild time axis from start date, depending on frequency and calendar."""
    num_axis = start + np.arange(length, step = inc)
    if not instant:
        num_axis+=0.5
    date_axis = num2date(num_axis, units = ctx.funits, calendar = ctx.calendar)
    axis = date2num(date_axis, units = ctx.tunits, calendar = ctx.calendar)
    return axis


def _rebuild_time_bnds(start, length, inc, ctx):
    """Rebuild time boundaries from start date, depending on frequency and calendar."""
    num_axis_bnds = np.column_stack(((start + np.arange(length, step = inc)),(start + np.arange(length+1, step = inc)[1:])))
    date_axis_bnds = num2date(num_axis_bnds, units = ctx.funits, calendar = ctx.calendar)
    axis_bnds = date2num(date_axis_bnds, units = ctx.tunits, calendar = ctx.calendar)    
    return axis_bnds


def _wrapper(args):
    """Wrapper multiple arguments into multiprocessing function."""
    def _time_axis_processing(filename, ctx):
        """Time axis process in three step: rebuild, check and rewrite."""
        # Extract start and end dates from filename
        start_date, end_date = _dates_from_filename(filename)
        start = date2num(start_date, units = ctx.funits, calendar = ctx.calendar)
        # Set time length, True/False instant axis and incrementation in frequency units
        data = Dataset(ctx.directory+filename, 'r+')
        length = data.variables['time'].shape[0]
        instant = _is_instant_time_axis(filename, ctx.realm)
        inc = _time_inc(ctx.frequency)
        # Instanciates object to display axis status
        print_ctx = _PrintingContext()
        print_ctx.file = filename
        print_ctx.start = start_date.strftime("%d/%m/%Y %H:%M:%S")
        print_ctx.end = end_date.strftime("%d/%m/%Y %H:%M:%S")
        print_ctx.steps = length
        if instant:
            print_ctx.instant = 'Yes'
        # Rebuild a proper time axis
        axis = _rebuild_time_axis(start, length, instant, inc, ctx)
        # Control consistency between last time date and end date from filename
        end = date2num(_dates_from_filename(filename)[1], units = ctx.tunits, calendar = ctx.calendar)
        last = axis[-1]-0.5
        if _last_date_checker(last, end):
            print_ctx.control = 'Processable'
            # Rebuild proper time boundaries if needed
            if 'time_bnds' in data.variables.keys():
                axis_bnds = _rebuild_time_bnds(start, length, inc, ctx)
            # Check time axis squareness
            if ctx.check or ctx.write:
                time = data.variables['time'][:]
                if _time_checker(axis, time):
                    print_ctx.time = 'Correct'
                else:
                    print_ctx.time = 'Mistaken'
                # Check time boundaries squareness if needed
                if 'time_bnds' in data.variables.keys():
                    time_bnds = data.variables['time_bnds'][:,:]
                    if _time_checker(axis_bnds, time_bnds): 
                        print_ctx.bnds = 'Correct'
                    else:
                        print_ctx.bnds = 'Mistaken'
            # Rewrite time axis depending on checking
            if (ctx.write and not _time_checker(axis, time)) or ctx.force:
                data.variables['time'][:] = axis
                # Rewrite time units according to CMIP5 requirements (i.e., same units for all files)
                data.variables['time'].units = ctx.tunits
                # Rewrite time boundaries if needed
                if 'time_bnds' in data.variables.keys():
                    data.variables['time_bnds'][:,:] = axis_bnds
        else:
            print_ctx.control = 'Ignored because last timestep {0} is inconsistent with end date filename: {1}'.format(last, end)
        # Close file
        data.close()
        # Return printing context
        return print_ctx
    return _time_axis_processing(*args)


class _PrintingContext(object):
    """Encapsulate printing context information."""
    def __init__(self):
        self.file = None
        self.start = None
        self.end = None
        self.steps = None
        self.instant = 'No'
        self.control = None
        self.time = None
        self.bnds = None


class _ProcessingContext(object):
    """Encapsulate processing context information."""
    def __init__(self, args):
        self.directory = args.directory
        self.check = args.check
        self.write = args.write
        self.force = args.force
        self.realm = None
        self.frequency = None
        self.calendar = None
        self.tunits = None
        self.funits = None
        self.is_verbose = args.verbose
        self.pool = ThreadPool(_THREAD_POOL_SIZE)


def main():
    """Main entry point."""
    # Initialise processing context
    ctx = _ProcessingContext(_get_args())
    # Control directory and filename syntax
    _control_directory(ctx)
    print ''.center(100,'=')
    print ''.center(100)
    print ' DIRECTORY:'.ljust(40)+'{0}'.format(ctx.directory).rjust(60)
    # Set driving time properties (calendar, frequency and time units) from first file in directory
    _time_init(ctx)
    sys.stdout.write('\r   |--> Files in process, please wait a minute...'.ljust(100)) ; sys.stdout.flush()
    # Process
    args = [(file,ctx) for file in _get_file_list(ctx)]
    infos = ctx.pool.map(_wrapper, args)
    sys.stdout.write('\r')
    # Print axis status
    for i in range(len(infos)):
        print '   |'.ljust(100)
        print '   |--> File:'.ljust(40)+'{0}'.format(infos[i].file).rjust(60)
        if ctx.is_verbose:
            print '   |     |--> Start:'.ljust(40)+'{0}'.format(infos[i].start).rjust(60)
            print '   |     |--> End:'.ljust(40)+'{0}'.format(infos[i].end).rjust(60)
            print '   |     |--> Timesteps:'.ljust(40)+'{0}'.format(infos[i].steps).rjust(60)
            print '   |     |--> Instant time:'.ljust(40)+'{0}'.format(infos[i].instant).rjust(60)
        print '   |     |--> Last date checking:'.ljust(40)+'{0}'.format(infos[i].control).rjust(60)
        print '   |     |--> Time axis checking:'.ljust(40)+'{0}'.format(infos[i].time).rjust(60)
        print '   |     |--> Time boundaries checking:'.ljust(40)+'{0}'.format(infos[i].bnds).rjust(60)
    print ''.center(100)
    print ''.center(100,'=')
    # Close thread pool
    ctx.pool.close()
    ctx.pool.join()


# Main entry point
if __name__ == "__main__":
    main()
