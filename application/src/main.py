from functions import m*
from logger import setup_logger

setup_logger()

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

if __name__ == "__main__":
    main()
