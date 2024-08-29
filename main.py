import copy
import time
import boto3
import psycopg2
from psycopg2 import sql

poll_interval = 10

def create_rds_instance(db_cluster_name, db_instance_name, db_name, db_user, db_master_password, db_subnet_group_name, security_group_id):
    rds = boto3.client('rds', region_name='us-east-1')
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
            time.sleep(poll_interval)

        print("Aurora Serverless cluster created:", response)

        instance_response = rds.create_db_instance(
            DBInstanceIdentifier=db_instance_name,
            DBClusterIdentifier=db_cluster_name,
            DBInstanceClass='db.r6g.large',  
            Engine='aurora-postgresql',
            AvailabilityZone='us-east-1a',  
            PubliclyAccessible=True,  
        )
        # Poll the instance until it becomes available
        while True:
            response = rds.describe_db_instances(
                DBInstanceIdentifier=db_instance_name
            )

            db_instance = response['DBInstances'][0]
            status = db_instance['DBInstanceStatus']
            print(f"Current instance status: {status}")

            if status == 'available':
                endpoint = db_instance['Endpoint']['Address']
                port = db_instance['Endpoint']['Port']
                print(f"DB instance is available. Endpoint: {endpoint}, Port: {port}")
                return endpoint

            print("DB instance not available yet, polling again...")
            time.sleep(poll_interval)  
        print("Aurora Serverless db instance created:", response)
    except Exception as e:
        print("Error creating Aurora Serverless instance:", str(e))

def delete_rds_instance(db_cluster_identifier):
    rds = boto3.client('rds', region_name='us-east-1')
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
            time.sleep(poll_interval)
    except rds.exceptions.DBClusterNotFoundFault:
        print(f'Successfully initiated deletion of RDS cluster: {db_cluster_identifier}')
    except Exception as e:
        print(f'Error deleting RDS instance: {str(e)}')

if __name__ == '__main__':
    # Create a new RDS instance inside a new RDS cluster
    db_cluster_name = "kontrol-plane-db-cluster-dev-four"
    db_instance_name = "instance-four"
    db_name = "kardinal"
    endpoint = create_rds_instance(db_cluster_name, db_instance_name, db_name, db_user, db_master_password, db_subnet_group_name, security_group_id)
    print(endpoint)
    endpoint = "instance-four.cvpzllhpfsxd.us-east-1.rds.amazonaws.com"

    # Make sure we can connect to the target RDS instance via pg client
    target_conn = psycopg2.connect(
        host=endpoint,
        dbname=db_name,
        user=db_user,
        password=db_master_password,
    )

    # Delete the RDS instance 
    # delete_rds_instance(db_name)

def create_flow(service_spec, deployment_spec, flow_uuid, db_user, db_master_password, db_subnet_group_name, security_group_id):
# def create_flow(service_spec, deployment_spec, flow_uuid):
    modified_deployment_spec = copy.deepcopy(deployment_spec)

    container = modified_deployment_spec['template']['spec']['containers'][0]
    env_vars = container.get('env', [])

    # Create a new RDS instance inside a new RDS cluster
    db_cluster_name = "kontrol-plane-db-cluster-dev"
    db_instance_name = "instance-one"
    db_name = next((env['value'] for env in env_vars if env['name'] == 'DB_NAME'), "postgres")
    endpoint = create_rds_instance(db_cluster_name, db_instance_name, db_name, db_user, db_master_password, db_subnet_group_name, security_group_id)
    print(endpoint)

    # Make sure we can connect to the target RDS instance via pg client
    target_conn = psycopg2.connect(
        host=endpoint,
        dbname=db_name,
        user=db_user,
        password=db_master_password,
    )

    # update modified deploymend spec with new container pointing to new hostname
    unchanged_env_vars = [
        env_var for env_var in env_vars
        if env_var['name'] != 'DB_HOSTNAME'  or env_var['name'] != 'DB_USER' or env_var['DB_PASSWORD']
    ]
    container['env'] = unchanged_env_vars + [
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