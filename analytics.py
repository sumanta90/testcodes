import json
from frictionless import describe, Resource, Pipeline, steps, transform
from frictionless.resources import TableResource

def read_analytics_json():
    with open("d&a.json","r") as inputFile:
        return json.loads(inputFile.read())
    
jsonData = read_analytics_json()

def get_node_details(node_number):
    for item in jsonData['nodes']:
        if item['node_id'] == node_number:
            return item
    return None
def get_frictionless_object(file_path):
    res = TableResource(path=f"{file_path}")
    print("RES: ", res.to_view())
    return res

def update_visited_node(node_id,set_data=None, set_steps=None, resource=None):
    for item in jsonData['nodes']:
        if item['node_id'] == node_id:
            if item.get('processed') is None:
                item['processed'] = True
            
            if set_steps:
                item['steps'] = set_steps
            
            if resource:
                item['resource'] = resource


def generate_transform_step(operation_data):
    if operation_data['operation_name'] == 'filter':
        return [
            steps.table_normalize(),
            steps.row_filter(formula=f"{operation_data['column_name']}{operation_data['operation_formula']}{operation_data['operation_value']}")
        ]

    if operation_data['operation_name'] == 'sum':
        return [
            steps.table_normalize(),
            steps.table_aggregate(
                group_name=operation_data['column_name'],
                aggregation={"sum": (f"{operation_data['column_name']}", sum)}
            )
        ]
    if operation_data['operation_name'] == 'min':
        return [
            steps.table_normalize(),
            steps.table_aggregate(
                group_name=operation_data['column_name'],
                aggregation={"min": (f"{operation_data['column_name']}", min)}
            )
        ]
        
def transform_and_write(operation, current_resource_node, target_node):
    dataset = current_resource_node.to_copy()
    # pipeline = Pipeline(steps=operation)
    print("Operation: ", operation)
    target = transform(dataset, steps=operation)
    print("COMPLETED: ", target.to_view(), "\n\n\n")
    
    target = target.write(target_node['data']['file_name'])

def update_load_node(node_id):
    for item in jsonData['nodes']:
        if item['node_id'] == node_id:
            if item.get('processed') is None:
                item['processed'] = True

                

master_pipeline = Pipeline()

for item in jsonData['edges']:
    source_node = get_node_details(item['source'])
    target_node = get_node_details(item['target'])
    
    
    # processing sources
    if source_node['data']['type'] == 'extract' and source_node.get('processed') == None:
        res = get_frictionless_object(source_node['data']['file_path'])
        update_visited_node(source_node['node_id'], resource=res)
        
    if source_node['data']['type'] == 'transform' and source_node.get('processed') == None:
        transform_step = generate_transform_step(source_node['data'])
        update_visited_node(source_node['node_id'], set_steps=transform_step)
        
    
    
    # processing targets
    if target_node['data']['type'] == 'transform' and target_node.get('processed') == None:
        transform_step = generate_transform_step(target_node['data'])
        if source_node['data']['type'] == 'transform':
            transform_step = source_node['steps'] + transform_step
        update_visited_node(target_node['node_id'], set_steps=transform_step, resource = source_node['resource'])
        
        
    # processing load
    if target_node['data']['type'] == 'load' and target_node.get('processed') == None:
        write_data = transform_and_write(source_node['steps'], source_node['resource'], target_node)
        update_load_node(target_node['node_id'])
        
