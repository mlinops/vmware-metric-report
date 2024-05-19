# Standard libraries
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import mean, StatisticsError
import traceback
import logging
import os
import calendar
import zipfile
import socket
import ssl

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
import base64

# Third-party libraries
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import pandas as pd
import concurrent.futures

# Custom libraries
from variables import *

# Main function
def main():

    try:
        filename = report_filename('clusters')
        vc_filename = report_filename('vcenters')

        columns_clusters = ['vCenter', 'Cluster', 'Hosts', 'CPU available', 'CPU allocated', 'CPU consumed', 'CPU oversub', 'RAM available', 'RAM allocated', 'RAM consumed']
        columns_vcenters = ['vCenter', 'Hosts', 'CPU available', 'CPU allocated', 'CPU consumed', 'RAM available', 'RAM allocated', 'RAM consumed']

        create_csv_file(filename, columns_clusters)
        create_csv_file(vc_filename, columns_vcenters)

        def process_vcenter(vcenter):

            session, content = vcenter_connect(vcenter)

            if session is None:
                return None, None

            try:
                container = content.viewManager.CreateContainerView(content.rootFolder, [vim.ClusterComputeResource], True)
                clusters_info = [get_clusters_info(content, vcenter, cluster) for cluster in container.view]

                if any(info is None for info in clusters_info):
                    logging.error('Error processing vCenter %s', vcenter)

                    return None, None
                
                vcenter_info = get_vcenter_info(vcenter, clusters_info)

                write_to_csv(filename, clusters_info, mode='a', header=False)
                write_to_csv(vc_filename, [vcenter_info], mode='a', header=False)

            finally:
                if session:
                    Disconnect(session)
                    logging.info('vCenter - %s - session closed.', vcenter)
            
            return 0
        
        parallel_execution(process_vcenter, VCENTERS)

        if datetime.now().month == 12:

            files = [f for f in os.listdir(REPORTS_PATH) if f.startswith("vcenters_") and f.endswith('.csv')]
            files = [os.path.basename(vc_filename)]

            resources = ['CPU consumed', 'RAM consumed']
            report_names = ['year_cpu_consumed', 'year_ram_consumed']

            for resource, report_name in zip(resources, report_names):
                unique_vcenters, vcenter_cpu_mapping = get_unique_vcenters(files, resource)
                if unique_vcenters and vcenter_cpu_mapping:
                    write_year_csv(unique_vcenters, vcenter_cpu_mapping, report_name)

            archive = archivation()
        else:
            pass

    except Exception as e:
        logging.error('Unexpected error: %s', str(e))
        logging.error(traceback.format_exc())

# Parallel execution of functions
def parallel_execution(func, items, *args, **kwargs):

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(func, items, *args, **kwargs))
    return results

# Creating a connection with vCenter
def vcenter_connect(vcenter):

    try:
        context = ssl.SSLContext()
        context.verify_mode = ssl.CERT_NONE

        session = SmartConnect(host=vcenter, user=LINOPS, pwd=PWD, port=443, sslContext=context)
        content = session.RetrieveContent()
        logging.info(f"vCenter - {vcenter} - session is open.")
        return session, content
    
    except vim.fault.InvalidLogin :
        logging.error(f'Failed to connect to vCenter "{vcenter}": incorrect username or password.')
        return None, None
    
    except socket.gaierror:
        logging.error(f'Failed to connect to vCenter "{vcenter}": address is unavailable, invalid or does not exist.')
        return None, None

    except Exception as e:
        logging.error(f'Failed to connect to vCenter "{vcenter}": {str(e)}')
        logging.error(traceback.format_exc())
        return None, None

# Obtaining information about the cluster
def get_clusters_info(content, vcenter, cluster):

    try:
        cluster_summary = {
                    'vcenter_name': vcenter,
                    'cluster_name': cluster.name,
                    'cluster_hosts': len(cluster.host),
                    'cpu_available (cores)': sum(host.hardware.cpuInfo.numCpuCores for host in cluster.host),
                    'cpu_allocated (cores)': sum(vm.config.hardware.numCPU for vm in cluster.resourcePool.vm),
                    'cpu_consumed (cores)': round((get_resources_consumed(6, content, cluster, 30) / 1000) * hosts_cpu_average(cluster)),
                    'cpu_oversubscription': get_cpu_oversubscription(cluster),
                    'ram_available (GB)': round(sum(host.hardware.memorySize / 1073741824 for host in cluster.host)),
                    'ram_allocated (GB)': round(sum(vm.config.hardware.memoryMB / 1024 for vm in cluster.resourcePool.vm)),
                    'ram_consumed (GB)': round(get_resources_consumed(98, content, cluster, 30) / 1024**2)
                }
            
    except Exception as e:
        logging.error('Error getting cluster info for %s in vCenter %s: %s', cluster.name, vcenter, str(e))
        logging.error(traceback.format_exc())
        return None   
    
    return cluster_summary


def get_cpu_oversubscription(cluster):

    cpu_available = sum(vm.config.hardware.numCPU for vm in cluster.resourcePool.vm)
    cpu_allocated = sum(host.hardware.cpuInfo.numCpuCores for host in cluster.host)

    cpu_oversubscription = cpu_available / cpu_allocated if cpu_allocated != 0 else 0

    return round(cpu_oversubscription, 1)

# Average speed of cluster processors
def hosts_cpu_average(cluster):

    try:
        return mean([host.summary.hardware.cpuMhz / 1000 for host in cluster.host])
    
    except StatisticsError as e:
        return 1

# Accumulating information about vCenters
def get_vcenter_info(vcenter, clusters_info):

    vcenter_info = {
                'vcenter_name': vcenter,
                'hosts': sum(entry['cluster_hosts'] for entry in clusters_info),
                'cpu_available': sum(entry['cpu_available (cores)'] for entry in clusters_info),
                'cpu_allocated': sum(entry['cpu_allocated (cores)'] for entry in clusters_info),
                # 'cpu_consumed (MHz)': sum(entry['cpu_consumed (MHz)'] for entry in clusters_info),
                'cpu_consumed (cores)': sum(entry['cpu_consumed (cores)'] for entry in clusters_info),
                'ram_available (GB)': sum(entry['ram_available (GB)'] for entry in clusters_info),
                'ram_allocated (GB)': sum(entry['ram_allocated (GB)'] for entry in clusters_info),
                # 'ram_consumed (KB)': sum(entry['ram_consumed (KB)'] for entry in clusters_info),
                'ram_consumed (GB)': sum(entry['ram_consumed (GB)'] for entry in clusters_info),
        }
    
    return vcenter_info

# Create the name for the report
def report_filename(report_name):
    try:

        return f"{REPORTS_PATH}{report_name}_{datetime.now():%d_%m_%Y_%H%M}.csv"

    except Exception as e:
        logging.error('Unexpected error: %s', str(e))
        logging.error(traceback.format_exc())
        return None

# Creating the report file
def create_csv_file(filename, columns):
    
    try:
        pd.DataFrame(columns=columns).to_csv(filename, index=False)

    except OSError as e:
        if not os.path.exists(REPORTS_PATH):
            os.makedirs(REPORTS_PATH)
            logging.warn(f'The "{REPORTS_PATH}" catalog was created because it didn\'t exist')

    except Exception as e:
        logging.error('Unexpected error: %s', str(e))
        logging.error(traceback.format_exc())
        return None

# Recording data to report
def write_to_csv(filename, data, mode='a', header=False):
    pd.DataFrame(data).to_csv(filename, mode=mode, header=header, index=False)

# Recording data to annual report
def write_year_csv(unique_vcenters, vcenter_cpu_mapping, report_name):

    header = ['vCenter'] + calendar.month_name[1:]
    data = []

    for vcenter in unique_vcenters:
        row = [vcenter] + [sum(vcenter_cpu_mapping[vcenter][month]) for month in calendar.month_name[1:]]
        data.append(row)

    df = pd.DataFrame(data, columns=header)
    df = df.fillna(0)  

    df.to_csv(report_filename(report_name), index=False)

# Getting resource data from reports
def get_unique_vcenters(files, resource):
    unique_vcenters = set()
    vcenter_cpu_mapping = defaultdict(lambda: defaultdict(list))

    for file in files:
        file_path = os.path.join(REPORTS_PATH, file)
        df = pd.read_csv(file_path, usecols=['vCenter', resource])

        _, date, month, year, time = file.split('_')
        month_name = calendar.month_name[int(month)]

        df.apply(lambda row: vcenter_cpu_mapping[row['vCenter']][month_name].append(row[resource]), axis=1)
        unique_vcenters.update(df['vCenter'].unique())

    return unique_vcenters, vcenter_cpu_mapping

# Archiving of reports for the year
def archivation():
    
    year = str(datetime.now().year)
    filename = f"{ARCHIVE_PATH}reports_{year}.zip"
        
    with zipfile.ZipFile(filename, 'w') as archive:
        for folder, _, files in os.walk(REPORTS_PATH):
            for file in files:
                if year in file:
                    filepath = os.path.join(folder, file)
                    archive.write(filepath)
                    os.remove(filepath)
    return filename

# Getting resource data for a month
def get_resources_consumed(counter_id, content, entity, interval_duration):

    try:
        metric_id = vim.PerformanceManager.MetricId(counterId=counter_id, instance="")
        end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=interval_duration) 
        query = vim.PerformanceManager.QuerySpec(
            entity=entity,
            metricId=[metric_id],
            format="normal",
            intervalId=7200,
            maxSample=300,
            startTime=start_time,
            endTime=end_time
        )
        performance_manager = content.perfManager
        stats = performance_manager.QueryPerf(querySpec=[query])

        if stats:
            cpu_values = []
            for sample in stats[0].value:
                cpu_values.extend(sample.value)
            return max(cpu_values)
        else:
            return 0
        
    except ValueError:
        return 0
    
    except Exception as e:
        print('Ошибка:\n', traceback.format_exc(e))
