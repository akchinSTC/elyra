#
# Copyright 2018-2021 Elyra Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import os

import pytest

from elyra.pipeline.validate import PipelineValidationManager
from elyra.pipeline.validate import ValidationResponse


@pytest.fixture
def load_pipeline():
    def _function(pipeline_filepath):
        response = ValidationResponse()

        with open(f'elyra/pipeline/tests/resources/validation_pipelines/{pipeline_filepath}') as f:
            pipeline = json.loads(f.read())
            return pipeline, response

    yield _function


@pytest.fixture
def create_validation_manager():
    yield PipelineValidationManager.instance()


def test_basic_pipeline_structure(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_basic_pipeline_only_notebook.pipeline')
    create_validation_manager._validate_pipeline_structure(pipeline=pipeline,
                                                           response=response)
    assert not response.has_fatal
    assert not response.to_json().get('issues')


def test_basic_pipeline_structure_with_scripts(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_basic_pipeline_with_scripts.pipeline')
    create_validation_manager._validate_pipeline_structure(pipeline=pipeline,
                                                           response=response)
    assert not response.has_fatal
    assert not response.to_json().get('issues')


async def test_invalid_runtime_node_kubeflow(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('kf_invalid_node_op.pipeline')
    node_id = "fe08b42d-bd8c-4e97-8010-0503a3185427"
    await create_validation_manager._validate_compatibility(pipeline=pipeline,
                                                            response=response,
                                                            pipeline_runtime='kfp',
                                                            pipeline_execution='kfp')

    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeType'
    assert issues[0]['data']['nodeID'] == node_id


async def test_invalid_runtime_node_kubeflow_with_supernode(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('kf_invalid_node_op_with_supernode.pipeline')
    node_id = "98aa7270-639b-42a4-9a07-b31cd0fa3205"
    pipeline_id = "00304a2b-dec4-4a73-ab4a-6830f97d7855"
    await create_validation_manager._validate_compatibility(pipeline=pipeline,
                                                            response=response,
                                                            pipeline_runtime='kfp',
                                                            pipeline_execution='kfp')
    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeType'
    assert issues[0]['data']['pipelineID'] == pipeline_id
    assert issues[0]['data']['nodeID'] == node_id


async def test_invalid_pipeline_runtime_with_kubeflow_execution(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_basic_pipeline_with_scripts.pipeline')

    await create_validation_manager._validate_compatibility(pipeline=pipeline,
                                                            response=response,
                                                            pipeline_runtime='airflow',
                                                            pipeline_execution='kfp')
    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidRuntime'


async def test_invalid_pipeline_runtime_with_local_execution(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_basic_pipeline_with_scripts.pipeline')

    await create_validation_manager._validate_compatibility(pipeline=pipeline,
                                                            response=response,
                                                            pipeline_runtime='airflow',
                                                            pipeline_execution='local')
    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidRuntime'
    assert issues[0]['data']['pipelineRuntime'] == 'airflow'


async def test_invalid_node_op_with_airflow(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('aa_invalid_node_op.pipeline')
    node_id = "593a5a87-5c48-4fc6-b471-03e6eb332d9c"
    await create_validation_manager._validate_compatibility(pipeline=pipeline,
                                                            response=response,
                                                            pipeline_runtime='airflow',
                                                            pipeline_execution='')
    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeType'
    assert issues[0]['data']['nodeID'] == node_id


async def test_invalid_node_property_structure(monkeypatch, load_pipeline):
    pipeline, response = load_pipeline('generic_invalid_node_property_structure.pipeline')
    node_id = '88ab83dc-d5f0-443a-8837-788ed16851b7'
    node_property = 'runtime_image'
    pvm = PipelineValidationManager.instance()

    monkeypatch.setattr(pvm, "_validate_filepath", lambda node, root_dir, property_name, filename, response: True)

    await pvm._validate_node_properties(root_dir='',
                                        pipeline=pipeline,
                                        response=response,
                                        pipeline_runtime='generic',
                                        pipeline_execution='kfp')

    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeProperty'
    assert issues[0]['data']['propertyName'] == node_property
    assert issues[0]['data']['nodeID'] == node_id


async def test_missing_node_property_for_kubeflow_pipeline(monkeypatch, load_pipeline):
    pipeline, response = load_pipeline('kf_invalid_node_property_in_component.pipeline')
    node_id = 'fe08b42d-bd8c-4e97-8010-0503a3185427'
    node_property = "notebook"
    pvm = PipelineValidationManager.instance()

    monkeypatch.setattr(pvm, "_validate_filepath", lambda node, root_dir, property_name, filename, response: True)

    await pvm._validate_node_properties(root_dir='',
                                        pipeline=pipeline,
                                        response=response,
                                        pipeline_runtime='kfp',
                                        pipeline_execution='')

    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeProperty'
    assert issues[0]['data']['propertyName'] == node_property
    assert issues[0]['data']['nodeID'] == node_id


def test_invalid_node_property_image_name(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_invalid_node_property_image_name.pipeline')
    node_id = '88ab83dc-d5f0-443a-8837-788ed16851b7'
    node_property = 'runtime_image'

    node = pipeline['pipelines'][0]['nodes'][0]

    create_validation_manager._validate_container_image_name(node, response)

    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeProperty'
    assert issues[0]['data']['propertyName'] == node_property
    assert issues[0]['data']['nodeID'] == node_id


def test_invalid_node_property_dependency_filepath_workspace(create_validation_manager):
    response = ValidationResponse()
    node = {"id": "test-id", "app_data": {"ui_data": {"label": "test"}}}
    property_name = 'test-property'

    create_validation_manager._validate_filepath(node=node, root_dir=os.getcwd(),
                                                 property_name=property_name,
                                                 filename='../invalid_filepath/to/file.ipynb',
                                                 response=response)
    issues = response.to_json().get('issues')
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidFilePath'
    assert issues[0]['data']['propertyName'] == property_name
    assert issues[0]['data']['nodeID'] == node['id']


def test_invalid_node_property_dependency_filepath_non_existent(create_validation_manager):
    response = ValidationResponse()
    node = {"id": "test-id", "app_data": {"ui_data": {"label": "test"}}}
    property_name = 'test-property'

    create_validation_manager._validate_filepath(node=node, root_dir=os.getcwd(),
                                                 property_name=property_name,
                                                 filename='invalid_filepath/to/file.ipynb',
                                                 response=response)
    issues = response.to_json().get('issues')
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidFilePath'
    assert issues[0]['data']['propertyName'] == property_name
    assert issues[0]['data']['nodeID'] == node['id']


def test_valid_node_property_dependency_filepath(create_validation_manager):
    response = ValidationResponse()
    valid_filename = 'elyra/pipeline/tests/resources/validation_pipelines/generic_single_cycle.pipeline'
    node = {"id": "test-id", "app_data": {"ui_data": {"label": "test"}}}
    property_name = 'test-property'

    create_validation_manager._validate_filepath(node=node, root_dir=os.getcwd(),
                                                 property_name=property_name,
                                                 filename=valid_filename,
                                                 response=response)

    assert not response.has_fatal
    assert not response.to_json().get('issues')


def test_invalid_node_property_resource_value(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_invalid_node_property_hardware_resources.pipeline')
    node_id = '88ab83dc-d5f0-443a-8837-788ed16851b7'

    node = pipeline['pipelines'][0]['nodes'][0]
    create_validation_manager._validate_resource_value(node, resource_name='memory', response=response)

    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeProperty'
    assert issues[0]['data']['propertyName'] == 'memory'
    assert issues[0]['data']['nodeID'] == node_id


def test_invalid_node_property_env_var(create_validation_manager):
    response = ValidationResponse()
    node = {"id": "test-id", "app_data": {"ui_data": {"label": "test"}}}
    invalid_env_var = "TEST_ENV_ONE\"test_one\""
    create_validation_manager._validate_environmental_variables(node=node,
                                                                env_var=invalid_env_var,
                                                                response=response)
    issues = response.to_json().get('issues')
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidEnvPair'
    assert issues[0]['data']['propertyName'] == 'env_vars'
    assert issues[0]['data']['nodeID'] == "test-id"


def test_invalid_node_property_ui_label(create_validation_manager):
    response = ValidationResponse()
    node = {"id": "test-id", "app_data": {"ui_data": {"label": "test"}}}
    invalid_label_name = "DEAD_BREAD_DEAD_BREAD_DEAD_BREAD_DEAD_BREAD_DEAD_BREAD_DEAD_BREAD_DEAD_BREAD"
    create_validation_manager._validate_ui_data_label(node=node,
                                                      label_name=invalid_label_name,
                                                      response=response)
    issues = response.to_json().get('issues')
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'invalidNodeLabel'
    assert issues[0]['data']['propertyName'] == 'label'
    assert issues[0]['data']['nodeID'] == "test-id"


def test_pipeline_graph_single_cycle(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_single_cycle.pipeline')
    cycle_ID = ['c309f6dd-b022-4b1c-b2b0-b6449bb26e8f', '8cb986cb-4fc9-4b1d-864d-0ec64b7ac13c']

    create_validation_manager._validate_pipeline_graph(pipeline=pipeline,
                                                       response=response)
    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'circularReference'
    assert issues[0]['data']['linkIDList'].sort() == cycle_ID.sort()


def test_pipeline_graph_double_cycle(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_double_cycle.pipeline')
    cycle_ID = ['597b2971-b95d-4df7-a36d-9d93b0345298', 'b63378e4-9085-4a33-9330-6f86054681f4']
    cycle_two_ID = ['c309f6dd-b022-4b1c-b2b0-b6449bb26e8f', '8cb986cb-4fc9-4b1d-864d-0ec64b7ac13c']

    create_validation_manager._validate_pipeline_graph(pipeline=pipeline,
                                                       response=response)
    issues = response.to_json().get('issues')
    assert len(issues) == 2
    assert issues[0]['severity'] == 1
    assert issues[0]['type'] == 'circularReference'
    assert issues[0]['data']['linkIDList'].sort() == cycle_ID.sort()
    assert issues[1]['severity'] == 1
    assert issues[1]['type'] == 'circularReference'
    assert issues[1]['data']['linkIDList'].sort() == cycle_two_ID.sort()


def test_pipeline_graph_singleton(create_validation_manager, load_pipeline):
    pipeline, response = load_pipeline('generic_singleton.pipeline')
    node_id = '0195fefd-3ceb-4a90-a12c-3958ef0ff42e'

    create_validation_manager._validate_pipeline_graph(pipeline=pipeline,
                                                       response=response)
    issues = response.to_json().get('issues')
    assert len(issues) == 1
    assert not response.has_fatal
    assert issues[0]['severity'] == 2
    assert issues[0]['type'] == 'singletonReference'
    assert issues[0]['data']['nodeID'] == node_id
