import copy

def create_flow(service_spec, deployment_spec, flow_uuid):
    modified_deployment_spec = copy.deepcopy(deployment_spec)

    return {
        "deployment_spec": modified_deployment_spec,
        "config_map": {
        }
    }
	
def delete_flow(config_map, flow_uuid):
    return