import time
import json
import boto3
import sys
import yaml
import os
import re
from datetime import datetime, timedelta
import concurrent.futures


def rds_metric():
   try:
      tasks = []
      metrics_dict = yaml.safe_load(open('rds_metric.yaml'))
      client = boto3.client("rds")
      instance_sets = client.describe_db_instances()
      cluster_sets = client.describe_db_clusters()
      
      cluster_batch_sets = []
      instance_batch_sets = []
      cluster_ones = []
      instance_ones = []
      for metric_type, m_type_l in metrics_dict.items():
         if metric_type == 'instance_metric':
            for engine_index in m_type_l:
               for metric_engine_type_name, metrics in engine_index.items():
                  for metric_name in metrics:
                     for instance_data in instance_sets['DBInstances']:
                           data = instance_data.get("Endpoint")
                           instance_name = instance_data['DBInstanceIdentifier']
                           engine = instance_data['Engine']
                           endpoint = data.get("Address")
                           allocatedStorage = instance_data.get("AllocatedStorage")
                           metric_name_prefix = 'aws_cloudwatch_rds_instance_'
                           if re.match(r'^' + metric_engine_type_name, engine):
                              task = dict()
                              task['metric_type'] = metric_type
                              task['engine_type_name'] = metric_engine_type_name
                              task['metric_name'] = metric_name
                              task['instance_name'] = instance_name
                              task['engine'] = engine
                              task['endpoint'] = endpoint
                              task['allocatedStorage'] = allocatedStorage
                              task['metric_name_prefix'] = metric_name_prefix
                              id_text = task['metric_type'] + '_' + task['instance_name'] + '_' + task['metric_name']
                              task['id'] = id_text.replace("_", '').replace("-", '')
                              task['Period'] = 300
                              task['param'] = {
                                                'Id': task['id'],
                                                'MetricStat': {
                                                   'Metric': {
                                                      'Namespace': 'AWS/RDS',
                                                      'MetricName': task['metric_name'],
                                                      'Dimensions': [{
                                                            'Name': 'DBInstanceIdentifier',
                                                            'Value': task['instance_name']
                                                            }]
                                                      },
                                                   'Period': task['Period'],
                                                   'Stat': 'Average'
                                                   }
                                                }
                              tasks.append(task)
                              instance_ones.append(task['param'])
                              if len(instance_ones) == 500:
                                 instance_batch_sets.append(instance_ones)
                                 instance_ones = []

         
         elif metric_type == 'cluster_metric':
            for metric_name in m_type_l:
               for cluster_data in cluster_sets['DBClusters']:
                  cluster_name = cluster_data['DBClusterIdentifier']
                  metric_name_prefix = 'aws_cloudwatch_rds_cluster_'
                  task = dict()
                  task['metric_type'] = metric_type
                  task['metric_name'] = metric_name
                  task['cluster_name'] = cluster_name
                  task['metric_name_prefix'] = metric_name_prefix
                  id_text = task['metric_type'] + '_' + task['cluster_name'] + '_' + task['metric_name']
                  task['id'] = id_text.replace("_", '').replace("-", '')
                  task['Period'] = 604800
                  task['param'] = {
                                    'Id': task['id'],
                                    'MetricStat': {
                                       'Metric': {
                                          'Namespace': 'AWS/RDS',
                                          'MetricName': task['metric_name'],
                                          'Dimensions': [{
                                                'Name': 'DBClusterIdentifier',
                                                'Value': task['cluster_name']
                                                }]
                                          },
                                       'Period': task['Period'],
                                       'Stat': 'Average'
                                       }
                                    }
                  tasks.append(task)
                  cluster_ones.append(task['param'])
                  if len(cluster_ones) == 500:
                     cluster_batch_sets.append(cluster_ones)
                     cluster_ones = []

      if len(instance_ones) != 0:
         instance_batch_sets.append(instance_ones)
      if len(cluster_ones) != 0:    
         cluster_batch_sets.append(cluster_ones)
      
      # get metric and create text
      texts = ''
      for i in instance_batch_sets:
         rt = get_metrics(i)
         print('instance', len(rt['MetricDataResults']))
         for rt_val in rt['MetricDataResults']:
            task = get_task(tasks, rt_val)
            if len(rt_val['Values']) != 0:
               time_stamp = str(int(time.time() * 1000))
               metric_value = rt_val['Values'][0]
               metric_name, metric_value = processing_rds_metric(task['metric_name'], metric_value, task)
               text = task['metric_name_prefix'] + metric_name + '{DBInstanceIdentifier="' + task['instance_name'] + '", Engine="' + task['engine']  + '"} ' + str(metric_value) + ' ' + time_stamp
               texts = texts + text + """
"""
            else:
               print(task['metric_name_prefix'] + task['metric_name'] + '{DBInstanceIdentifier="' + task['instance_name'] + '"} ' + 'no data ')

      for i in cluster_batch_sets:
         rt = get_metrics(i)
         print('cluster', len(rt['MetricDataResults']))
         for rt_val in rt['MetricDataResults']:
            task = get_task(tasks, rt_val)
            if len(rt_val['Values']) != 0:
               time_stamp = str(int(time.time() * 1000))
               metric_value = rt_val['Values'][0]
               metric_name, metric_value = processing_rds_metric(task['metric_name'], metric_value, task)
               text = task['metric_name_prefix'] + metric_name + '{DBClusterIdentifier="' + task['cluster_name'] + '"} ' + str(metric_value) + ' ' + time_stamp
               texts = texts + text + """
"""
            else:
               print(task['metric_name_prefix'] + task['metric_name'] + '{DBClusterIdentifier="' + task['cluster_name'] + '"} ' + 'no data ')

      return texts

   except Exception as e:
      print('rds_metric: ', e)



def get_task(tasks, metric_rt):
   rt = None
   for i in tasks:
      if i['id'] == metric_rt['Id']:
         rt = i
         break
   return rt
   




def get_metrics(task, period=300):
   try:
      cloudwatch_client = boto3.client("cloudwatch")
      end = datetime.now()
      Period = period
      start = end - timedelta(seconds=Period)
      metrics = cloudwatch_client.get_metric_data(MetricDataQueries=task,
                                                      StartTime=start,
                                                      EndTime=end)

      return metrics
   except Exception as e:
      return None





def processing_rds_metric(metric_name, metrics, task):
   try:
      val = metrics
      m_name = metric_name
      if metric_name == 'FreeStorageSpace' and metrics is not None:
         global allocatedStorage
         m_name = 'UsedStorageSpace'
         val = (1 - (metrics / 1024 / 1024 / 1024 / task['allocatedStorage']) ) * 100

      if val is not None:
         val = format(round(val, 3), '30.3f').strip()
   except Exception as e:
      print('processing_metric: ', e)

   return m_name, val







def batch_execute_async(func_obj, data, concurry_num=7, chunksize=1):
   p_list = []
   reponse_list = []

   try:
      executor = concurrent.futures.ProcessPoolExecutor(max_workers=concurry_num)

      for i in data:
         p_list.append(executor.map(func_obj, [i], chunksize=chunksize))  

      for r_obj in p_list:
         for r in r_obj:
               reponse_list.append(r)

   except Exception as e:
      print(func_obj, data, e)
   
   return reponse_list
 
 
 
 