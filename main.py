import copy
import time
import boto3
import psycopg2
from psycopg2 import sql

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html

rds = boto3.client('rds', region_name='us-east-1')

# TODO: figure out if its possible to create these and get rid of these inside the plugin
# subnet_ids = ['subnet-03bd6e61cef861c5a', 'subnet-0f4a82d2d643763ad']  
security_group_id = 'sg-0a71103300fdaaca1'
db_subnet_group_name = 'public-subnets'

# TODO: figure out how to get these inside the plugin
db_user = "tedi"
db_master_password= "X7MMcKwVbNMPcGgPfkLn"

def create_rds_instance(db_cluster_name, db_instance_name, db_name, db_user, db_master_password, db_subnet_group_name, security_group_id):
    try:
        response = rds.create_db_cluster(
            DatabaseName=db_name,
            DBClusterIdentifier=db_cluster_name,
            Engine='aurora-postgresql',
            MasterUsername=db_user,
            MasterUserPassword=db_master_password,
            DBSubnetGroupName=db_subnet_group_name,
            VpcSecurityGroupIds=[
                security_group_id
            ],
        )
        while True:
            try:
                response = rds.describe_db_clusters(DBClusterIdentifier=db_cluster_name)
                status = response['DBClusters'][0]['Status']
                print(f"Current status of the cluster '{db_cluster_name}': {status}")
                
                if status == 'available':
                    print(f"The cluster '{db_cluster_name}' is now available.")
                    break
                elif status in ['deleting', 'failed', 'incompatible-restore', 'incompatible-network']:
                    print(f"Cluster creation failed with status: {status}")
                    break
            except Exception as e:
                print("Error polling cluster status:", str(e))
                break
            print("cluster not available yet, polling again...")
            time.sleep(10)

        print("Aurora Serverless cluster created:", response)

        instance_response = rds.create_db_instance(
            DBInstanceIdentifier=db_instance_name,
            DBClusterIdentifier=db_cluster_name,
            DBInstanceClass='db.r6g.large',  
            Engine='aurora-postgresql',
            AvailabilityZone='us-east-1a',  
            PubliclyAccessible=True,  
        )
        while True:
            response = rds.describe_db_instances(
                Filters=[
                    {
                        'Name': 'db-cluster-id',
                        'Values': [db_cluster_name]
                    }
                ]
            )

            # TODO: Improve availability logic for instances
            if len(response['DBInstances']) > 0:
                for db_instance in response['DBInstances']:
                    if db_instance['PubliclyAccessible']:
                        endpoint = db_instance['Endpoint']['Address']
                        port = db_instance['Endpoint']['Port']
                        return endpoint, port
            print("no instances in cluster yet, polling again...")
            time.sleep(10)
        print("Aurora Serverless db instance created:", response)
    except Exception as e:
        print("Error creating Aurora Serverless cluster:", str(e))

def delete_rds_instance(db_cluster_identifier):
    try:
        response = rds.delete_db_cluster(
            DBClusterIdentifier=db_cluster_identifier,
            SkipFinalSnapshot=True,  
        )

        while True:
            response = rds.describe_db_clusters(DBClusterIdentifier=db_cluster_identifier)
            db_clusters = response['DBClusters']

            if not db_clusters:
                print(f"DB cluster {db_cluster_identifier} does not exist.")
                break

            status = db_clusters[0]['Status']
            print(f"DB cluster status: {status}")

            if status == 'deleting':
                print(f"DB cluster {db_cluster_identifier} is still being deleted. Waiting...")
            else:
                print(f"DB cluster {db_cluster_identifier} is in unexpected status: {status}")
                break
            print("cluster not deleted yet, waiting...")
            time.sleep(10)
    except rds.exceptions.DBClusterNotFoundFault:
        print(f'Successfully initiated deletion of RDS cluster: {db_cluster_identifier}')
    except Exception as e:
        print(f'Error deleting RDS instance: {str(e)}')

if __name__ == '__main__':
    # Create a new RDS instance inside a new RDS cluster
    db_cluster_name = "kontrol-plane-db-cluster-dev-three"
    db_instance_name = "instance-one"
    db_name = "kardinal"
    endpoint = create_rds_instance(db_cluster_name, db_instance_name, db_name, db_user, db_master_password, db_subnet_group_name, security_group_id)
    print(endpoint)

    # Make sure we can connect to the target RDS instance via pg client
    target_conn = psycopg2.connect(
        host=endpoint,
        dbname=db_name,
        user=db_user,
        password=db_master_password,
    )

    # Delete the RDS instance (Uncomment the following line to delete the instance)
    # delete_rds_instance(db_name)

# def create_flow(service_spec, deployment_spec, flow_uuid, db_cluster_name, db_name, db_user, db_master_password, db_subnet_group_name, security_group_id):
def create_flow(service_spec, deployment_spec, flow_uuid):
    modified_deployment_spec = copy.deepcopy(deployment_spec)

    # Create a new RDS instance inside a new RDS cluster
    db_cluster_name = "kontrol-plane-db-cluster-dev-four"
    db_instance_name = "instance-one"
    db_name = "kardinal"
    endpoint = create_rds_instance(db_cluster_name, db_instance_name, db_name, db_user, db_master_password, db_subnet_group_name, security_group_id)
    print(endpoint)

    # Make sure we can connect to the target RDS instance via pg client
    target_conn = psycopg2.connect(
        host=endpoint,
        dbname=kardinal,
        user=db_user,
        password=db_master_password,
    )

    # update modified deploymend spec with new container pointing to new hostname
    container = modified_deployment_spec['template']['spec']['containers'][0]
    existing_env_vars = [
        env_var for env_var in container.get('env', [])
        if env_var['name'] != 'DB_HOSTNAME'  or env_var['name'] != 'DB_USER' or env_var['DB_PASSWORD']
    ]
    container['env'] = existing_env_vars + [
        {'name': 'DB_HOSTNAME', 'value': endpoint},
        {'name': 'DB_USER', 'value': db_user},
        {'name': 'DB_PASSWORD', 'value': db_master_password},
    ]

    modified_deployment_spec['template']['spec']['containers'] = [container]

    return {
        "deployment_spec": modified_deployment_spec,
        "config_map": {
            "DB_CLUSTER_NAME": db_cluster_name,
        }
    }
	
def delete_flow(config_map, flow_uuid):
    delete_rds_instance(config_map["DB_CLUSTER_NAME"])
    return