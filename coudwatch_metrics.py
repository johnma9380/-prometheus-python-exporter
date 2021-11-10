import time
import json
import boto3
import yaml
import os
from datetime import datetime, timedelta


def rds_metric():
   text = """
"""
   metrics_dict = yaml.safe_load(open('rds_metric.yaml'))

   client = boto3.client("rds")
   instance_sets = client.describe_db_instances()
   cluster_sets = client.describe_db_clusters()
   for k, v in metrics_dict.items():
      if k == 'instance_metric':
         for i in instance_sets['DBInstances']:
            extra_data = dict()
            data = i.get("Endpoint")
            extra_data['instance_name'] = i['DBInstanceIdentifier']
            extra_data['engine'] = i['Engine']
            extra_data['endpoint'] = data.get("Address")
            extra_data['allocatedStorage'] = i.get("AllocatedStorage")
            metric_name_prefix = 'aws_cloudwatch_rds_instance_'
            for m in v:
                metric_name = m
                time_stamp = str(int(time.time() * 1000))
                metric_value = get_metrics(metric_name, 'AWS/RDS', 'DBInstanceIdentifier', extra_data['instance_name'], 'Average')
                if metric_value is not None:
                    metric_name, metric_value = processing_metric(metric_name, metric_value, extra_data)
                    text = text + metric_name_prefix + metric_name + '{ DBInstanceIdentifier="' + extra_data['instance_name'] + '" } ' + str(metric_value) + ' ' + time_stamp + """
"""
      elif k == 'cluster_metric':
         for i in cluster_sets['DBClusters']:
            extra_data = dict()
            extra_data['cluster_name'] = i['DBClusterIdentifier']
            metric_name_prefix = 'aws_cloudwatch_rds_cluster_'
            for m in v:
                metric_name = m
                time_stamp = str(int(time.time() * 1000))
                metric_value = get_metrics(metric_name, 'AWS/RDS', 'DBClusterIdentifier', extra_data['cluster_name'], 'Average')
                if metric_value is not None:
                    metric_name, metric_value = processing_metric(metric_name, metric_value, extra_data)
                    text = text + metric_name_prefix + metric_name + '{ DBClusterIdentifier="' + extra_data['cluster_name'] + '" } ' + str(metric_value) + ' ' + time_stamp + """
"""

   return text


def get_metrics(metric_name, namespace, dimension_column, dimension_value, statistics):
    cloudwatch_client = boto3.client("cloudwatch")
    end = datetime.now()
    Period = 120
    start = end - timedelta(seconds=Period)
    metrics = cloudwatch_client.get_metric_data(
        MetricDataQueries=[{
                        'Id': 'pythongetmetric',
                        'MetricStat': {
                            'Metric': {
                                'Namespace': namespace,
                                'MetricName': metric_name,
                                'Dimensions': [{
                                    'Name': dimension_column,
                                    'Value': dimension_value
                                    }]
                                },
                            'Period': Period,
                            'Stat': statistics
                            }
                        }],
                        StartTime=start,
                        EndTime=end
                        )


    val = metrics['MetricDataResults'][0]['Values']
    if len(val) == 0:
        val = None
    else:
        val = val[0]

    return val



def processing_metric(metric_name, metrics, extra_data):
    if metric_name == 'FreeStorageSpace' and metrics is not None:
        global allocatedStorage
        m_name = 'UsedStorageSpace'
        return m_name, (1 - (metrics / 1024 / 1024 / 1024 / extra_data['allocatedStorage']) ) * 100
    m_name = metric_name
    return m_name, metrics