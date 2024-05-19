import os

VCENTERS = ['vcenter-01.example.com', 'vcenter-02.example.com', 'vcenter-03.example.com']

LINOPS = os.environ.get('VSPHERE_LOGIN') # CI variable
PWD = os.environ.get('VSPHERE_PWD') # CI variable

# Reports

REPORTS_PATH = './application/reports/'
ARCHIVE_PATH = './application/archives/'

# Logging

LOG_FILE = './application/logs/general.log'
LOGGING_LEVEL = 'DEBUG'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'

# Mail

SMTP_SERVER = '10.10.10.10'
SMTP_PORT = 587
SMTP_USERNAME = os.environ.get('SMTP_LOGIN') # CI variable
SMTP_PWD = os.environ.get('SMTP_PASS') # CI variable

SENDER = os.environ.get('SMTP_LOGIN')
RECIPIENT = os.environ.get('RECIPIENT')
SUBJECT = 'vSphere report'
