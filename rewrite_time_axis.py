#!/usr/bin/env python

##################
### LIBRAIRIES ###
##################

import re, sys, os, argparse
import cdms2 as cdms
import cdtime
import numpy as np
from datetime import datetime
from argparse import RawTextHelpFormatter

#################
### FUNCTIONS ###
#################

def printFormat(text, variable) :
   """Print text line, left justify, with optional variable."""
   print text.ljust(32,'-')+'> %s' % variable

def DatesFromFilename(filename) :
   """Read start/end period dates from CMIP5 filename."""
   pattern = re.compile(r'([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\d]+)-([\d]+).nc')
   start, end = pattern.search(file).group(6), pattern.search(file).group(7)
   if len(start) == len(end) == 4 : # Yearly file
      date_format = "%Y"
   elif len(start) == len(end) == 6 : # Monthly file
      date_format = "%Y%m"
   elif len(start) == len(end) == 8 : # Daily files
      date_format = "%Y%m%d"
   elif len(start) == len(end) == 10 : # 3-6 hourly file
      date_format = "%Y%m%d%H" 
   elif len(start) == len(end) == 12 : # 3-6 hourly file
      date_format = "%Y%m%d%H%M"
   elif len(start) == len(end) == 14 : # Sub-hourly file
      date_format = "%Y%m%d%H%M%S"
   else :
      print 'ERROR: Invalid number of dates digits (%s digits is not supported).' % len(start)
      sys.exit(1)
   start = datetime.strptime(start, date_format)
   end   = datetime.strptime(end, date_format)
   return start, end

def DateConvert(date) :
   """Convert datetime.datetime object into cdtime.comptime object."""
   if isinstance(date, datetime) :
      return cdtime.comptime(date.year, date.month, date.day, date.hour, date.minute, date.second)
   else :
      print 'ERROR: Invalid datetime object.'
      sys.exit(1)

def isInstantTimeAxis(file) :
   """Return True or Flase respectively if file required an instant time axis or not."""
   # Defining all (variable, CMOR table, realm) tuples needing instant time axis
   need_instant_time = [('tas','3hr','atmos'),('uas','3hr','atmos'),('vas','3hr','atmos'),('huss','3hr','atmos'),('mrsos','3hr','land'),('tslsi','3hr','land'),('tso','3hr','ocean'),('ps','3hr','atmos'),('ua','6hrPlev','atmos'),('va','6hrPlev','atmos'),('ta','6hrPlev','atmos'),('psl','6hrPlev','atmos'),('clcalipso','cf3hr','atmos'),('clcalipso2','cf3hr','atmos'),('cfadDbze94','cf3hr','atmos'),('cfadLidarsr532','cf3hr','atmos'),('parasolRefl','cf3hr','atmos'),('cltcalipso','cf3hr','atmos'),('cllcalipso','cf3hr','atmos'),('clmcalipso','cf3hr','atmos'),('clhcalipso','cf3hr','atmos'),('lon','cf3hr','atmos'),('lat','cf3hr','atmos'),('tas','cf3hr','atmos'),('ts','cf3hr','atmos'),('tasmin','cf3hr','atmos'),('tasmax','cf3hr','atmos'),('psl','cf3hr','atmos'),('ps','cf3hr','atmos'),('uas','cf3hr','atmos'),('vas','cf3hr','atmos'),('sfcWind','cf3hr','atmos'),('hurs','cf3hr','atmos'),('huss','cf3hr','atmos'),('pr','cf3hr','atmos'),('prsn','cf3hr','atmos'),('prc','cf3hr','atmos'),('evspsbl','cf3hr','atmos'),('sbl','cf3hr','atmos'),('tauu','cf3hr','atmos'),('tauv','cf3hr','atmos'),('hfls','cf3hr','atmos'),('hfss','cf3hr','atmos'),('rlds','cf3hr','atmos'),('rlus','cf3hr','atmos'),('rsds','cf3hr','atmos'),('rsus','cf3hr','atmos'),('rsdscs','cf3hr','atmos'),('rsuscs','cf3hr','atmos'),('rldscs','cf3hr','atmos'),('rsdt','cf3hr','atmos'),('rsut','cf3hr','atmos'),('rlut','cf3hr','atmos'),('rlutcs','cf3hr','atmos'),('rsutcs','cf3hr','atmos'),('prw','cf3hr','atmos'),('clt','cf3hr','atmos'),('clwvi','cf3hr','atmos'),('clivi','cf3hr','atmos'),('rtmt','cf3hr','atmos'),('ccb','cf3hr','atmos'),('cct','cf3hr','atmos'),('ci','cf3hr','atmos'),('sci','cf3hr','atmos'),('fco2antt','cf3hr','atmos'),('fco2fos','cf3hr','atmos'),('fco2nat','cf3hr','atmos'),('cltc','cf3hr','atmos'),('zfull','cf3hr','atmos'),('zhalf','cf3hr','atmos'),('pfull','cf3hr','atmos'),('phalf','cf3hr','atmos'),('ta','cf3hr','atmos'),('h2o','cf3hr','atmos'),('clws','cf3hr','atmos'),('clis','cf3hr','atmos'),('clwc','cf3hr','atmos'),('clic','cf3hr','atmos'),('reffclws','cf3hr','atmos'),('reffclis','cf3hr','atmos'),('reffclwc','cf3hr','atmos'),('reffclic','cf3hr','atmos'),('grpllsprof','cf3hr','atmos'),('prcprof','cf3hr','atmos'),('prlsprof','cf3hr','atmos'),('prsnc','cf3hr','atmos'),('prlsns','cf3hr','atmos'),('reffgrpls','cf3hr','atmos'),('reffrainc','cf3hr','atmos'),('reffrains','cf3hr','atmos'),('reffsnowc','cf3hr','atmos'),('reffsnows','cf3hr','atmos'),('dtaus','cf3hr','atmos'),('dtauc','cf3hr','atmos'),('dems','cf3hr','atmos'),('demc','cf3hr','atmos'),('clc','cf3hr','atmos'),('cls','cf3hr','atmos'),('tas','cfSites','atmos'),('ts','cfSites','atmos'),('psl','cfSites','atmos'),('ps','cfSites','atmos'),('uas','cfSites','atmos'),('vas','cfSites','atmos'),('sfcWind','cfSites','atmos'),('hurs','cfSites','atmos'),('huss','cfSites','atmos'),('pr','cfSites','atmos'),('prsn','cfSites','atmos'),('prc','cfSites','atmos'),('evspsbl','cfSites','atmos'),('sbl','cfSites','atmos'),('tauu','cfSites','atmos'),('tauv','cfSites','atmos'),('hfls','cfSites','atmos'),('hfss','cfSites','atmos'),('rlds','cfSites','atmos'),('rlus','cfSites','atmos'),('rsds','cfSites','atmos'),('rsus','cfSites','atmos'),('rsdscs','cfSites','atmos'),('rsuscs','cfSites','atmos'),('rldscs','cfSites','atmos'),('rsdt','cfSites','atmos'),('rsut','cfSites','atmos'),('rlut','cfSites','atmos'),('rlutcs','cfSites','atmos'),('rsutcs','cfSites','atmos'),('prw','cfSites','atmos'),('clt','cfSites','atmos'),('clwvi','cfSites','atmos'),('clivi','cfSites','atmos'),('rtmt','cfSites','atmos'),('ccb','cfSites','atmos'),('cct','cfSites','atmos'),('ci','cfSites','atmos'),('sci','cfSites','atmos'),('fco2antt','cfSites','atmos'),('fco2fos','cfSites','atmos'),('fco2nat','cfSites','atmos'),('cl','cfSites','atmos'),('clw','cfSites','atmos'),('cli','cfSites','atmos'),('mc','cfSites','atmos'),('ta','cfSites','atmos'),('ua','cfSites','atmos'),('va','cfSites','atmos'),('hus','cfSites','atmos'),('hur','cfSites','atmos'),('wap','cfSites','atmos'),('zg','cfSites','atmos'),('rlu','cfSites','atmos'),('rsu','cfSites','atmos'),('rld','cfSites','atmos'),('rsd','cfSites','atmos'),('rlucs','cfSites','atmos'),('rsucs','cfSites','atmos'),('rldcs','cfSites','atmos'),('rsdcs','cfSites','atmos'),('tnt','cfSites','atmos'),('tnta','cfSites','atmos'),('tntmp','cfSites','atmos'),('tntscpbl','cfSites','atmos'),('tntr','cfSites','atmos'),('tntc','cfSites','atmos'),('tnhus','cfSites','atmos'),('tnhusa','cfSites','atmos'),('tnhusc','cfSites','atmos'),('tnhusd','cfSites','atmos'),('tnhusscpbl','cfSites','atmos'),('tnhusmp','cfSites','atmos'),('evu','cfSites','atmos'),('edt','cfSites','atmos'),('pfull','cfSites','atmos'),('phalf','cfSites','atmos')]
   pattern = re.compile(r'([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\d]+)-([\d]+).nc')
   if (pattern.search(file).group(1), pattern.search(file).group(2), f.modeling_realm) in need_instant_time :
      return True
   else :
      return False

def TimeInc(frequency) :
   """Return tuple used for time incrementation depending on frequency: (raising value, time unit)."""
   unit = {'subhr': (30, cdtime.Minutes),
           '3hr'  : (3,  cdtime.Hours),
           '6hr'  : (6,  cdtime.Hours),
           'day'  : (1,  cdtime.Days),
           'mon'  : (1,  cdtime.Month),
           'yr'   : (1,  cdtime.Years)}
   if frequency not in unit.keys() :
      print 'ERROR: Invalid frequency (%s is not supported).' % frequency
      sys.exit(1)
   return unit[frequency]

def TimeChecker(right_axis, test_axis) :
   """Return True or False respectively if time axis is right or wrong."""
   if np.array_equal(right_axis, test_axis) :
      return True
   else :
      return False

######################
### COMMAND PARSER ###
######################

# Program version
__version__ = 'v0.1 '+'-'.join(['2014','05','12'])

# Parse command-line arguments
parser = argparse.ArgumentParser( \
   description='Rewrite and/or check CMIP5 file time axis on CICLAD filesystem, considering (i) uncorrupted filename period dates and (ii) properly-defined times units, time calendar and frequency NetCDF attributes.',\
   epilog = 'Developped by G. Levavasseur (CNRS/IPSL)',\
   formatter_class = RawTextHelpFormatter\
   )
parser.add_argument('directory', nargs = '?', help = 'Path to browse following CMIP5 DRS to the variable facet in latest directory (e.g., /prodigfs/esg/CMIP5/merge/NCAR/CCSM4/amip/day/atmos/cfDay/r7i1p1/latest/ps/)')
parser.add_argument('-c', '--check', action = 'store_true', default = False, help = 'Check time axis squareness.')
parser.add_argument('-w', '--write', action = 'store_true', default = False, help = 'Rewrite time axis depending on checking (including -c/--check). \nTHIS ACTION DEFINITELY MODIFY INPUT FILES !')
parser.add_argument('-f', '--force', action = 'store_true', default = False, help = 'Force time axis writing overpassing checking. \nTHIS ACTION DEFINITELY MODIFY INPUT FILES !')
parser.add_argument('-v', '--verbose', action = 'store_true', default = False, help = 'Verbose mode')
parser.add_argument('-V', '--version', action = 'version', version = '%(prog)s ('+__version__+')', help = 'Program version')
args = parser.parse_args()

############
### MAIN ###
############

# Verbose mode
verbose = args.verbose

### CONTROL DIRECTORY SYNTAX ###
path = args.directory
#dir_pattern = re.compile(r'/prodigfs/esg/CMIP5/merge/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/([\w.-]+)/latest/([\w.-]+)/')
dir_pattern = re.compile(r'/data/glipsl/test_time_axis/([\w.-]+)/')
if re.match(dir_pattern, path) == None :
   print 'ERROR: Invalid directory (%s does not follow CMIP5 DRS.)' % path
   sys.exit(1)

### BROWSE DIRECTORY ###
files = sorted(os.listdir(path))
for file in files :

   ### CONTROL FILENAME SYNTAXE ###
   file_pattern = re.compile(r'([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\w.-]+)_([\d]+)-([\d]+).nc')
   if re.match(file_pattern, file) == None : 
      print 'ERROR: Invalid filename (%s does not follow CMIP5 DRS).' % file
      continue

   #### READING FILE ###
   print 'File: %s' % path+file
   if args.write or args.force :
      f = cdms.open(path+file,'r+')
   else :
      f = cdms.open(path+file,'r')
   time = f['time'] ; time_values = time.getValue()
   if 'time_bnds' in f.variables.keys() :
      time_bnds = f['time_bnds']

   ### INTIALIZE FREQUENCY, TIME UNITS AND CDMS CALENDAR ###
   # Frequency, time units and time calendar have to persist from the first read file to the next.
   if file is files[0] :
      freq = f.frequency
      time_units = time.units
      if verbose :
         printFormat('  Reference frequency ', freq)
         printFormat('  Reference time units ', time_units)
      if time.getCalendar() == 'None' :
         print 'ERROR: Unknown calendar (%s is not supported).' % time.calendar
      else :
         cdtime.DefaultCalendar = time.getCalendar()
         if verbose :
            printFormat('  Initialize CDMS calendar ', time.calendar)

   ### DEFINE TIME AXIS ### 
   # Axis parameters depends on frequency and time axis type (instant or averaged)
   inc    = TimeInc(freq)[0] # Time increment
   unit   = TimeInc(freq)[1] # Time unit
   if verbose :
      printFormat('  Instant time axis ', isInstantTimeAxis(file))

   ### DEFINE CDMS START AND END PERIOD DATES FROM FILENAME ###
   start, end = DatesFromFilename(file)
   start, end = DateConvert(start), DateConvert(end)
   if verbose :
      printFormat('  File goes from ', start)
      printFormat('              to ', end)

   ### COMPUTE TIME AXIS AND BOUNDARIES ###
   # Following time unit and previous default calendar:
   # To avoid any hidden time error from CDAT, time axis is building from time boundaries, simply following time unit and previous default calendar:
   # start_bnds = timestep x inc
   # end_bnds   = (timestep + 1) x inc
   #
   # Then, time axis always follows the scheme:
   # time[t] = cumsum(bnds_diff[:t]) + bnds_diff[t]/2
   if args.check or args.write or args.force :
      sys.stdout.write('  Compute time axis properly '.ljust(32,'-')) ; sys.stdout.flush()
      axis      = np.zeros(len(time), float)
      axis_bnds = np.zeros((len(time), 2), float)
      for t in range(len(time)) :
         axis_bnds[t,0] = start.torel(time_units).add(t * inc, unit).value
         axis_bnds[t,1] = start.torel(time_units).add((t+1) * inc, unit).value
         axis[t] = axis_bnds[0,0] + np.sum(axis_bnds[:t,1] - axis_bnds[:t,0]) + (axis_bnds[t,1] - axis_bnds[t,0]) / 2
      if isInstantTimeAxis(file) :
         axis[t] = axis_bnds[t,1]
      print '> Done'

   ### CHECK TIME AXIS, TIME BOUNDARIES AND END DATE ###
   if args.check or args.write :
      printFormat('  Time boundaries exist ', 'time_bnds' in f.variables.keys()) 
      sys.stdout.write('  Check file time boundaries '.ljust(32,'-')) ; sys.stdout.flush()
      check = TimeChecker(axis_bnds, time_bnds)
      if check : 
         print '> Correct'
      else :
         print '> Mistaken'
      sys.stdout.write('  Check file time axis '.ljust(32,'-')) ; sys.stdout.flush()
      check = TimeChecker(axis, time_values)
      if check : 
         print '> Correct'
      else :
         print '> Mistaken'
      # Basic control of last time date with the end date from filename
      last = start.torel(time_units).add((len(time)-1) * inc, unit).tocomp()
      if last != end :
         print 'ERROR: End date inconsistent with filename (%s instead of %s)' % (last, end)
         sys.exit(1)

   ### REWRITE TIME AXIS, TIME BOUNDARIES AND TIME UNITS (IF NECESSARY OR FORCED) ###
   if (args.write and not check) or args.force :
      sys.stdout.write('  Rewrite time axis '.ljust(32,'-')) ; sys.stdout.flush() ;
      time.assignValue(axis)
      time.units = time_units
      if ('time_bnds' in f.variables.keys()) :
         time_bnds.assignValue(axis_bnds)
      print '> Done'

   ### CLOSE FILE ###
   print ''
   f.close()
      

