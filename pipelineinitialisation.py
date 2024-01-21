import requests
import json
import numpy as np
import random
import string
import re
from git import Repo
import os
import time
import yaml
from yaml.composer import ComposerError
from yaml.error import MarkedYAMLError
from kubernetes import client, config
from kubernetes.client.rest import ApiException


user_account = os.getenv('user-account')
pvc_name = os.getenv('pvc-name')
repo_name = os.getenv('repo-name')
folder_name = os.getenv('folder-name')
custom_container_prefix = os.getenv('custom-container-prefix')


def get_configmaps():
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    try:
        configmaps = v1.list_namespaced_config_map(namespace='admin')
    except Exception as e:
        print(f"Error: {e}")

    configmaplist = []

    for configmap in configmaps.items:
        print('Configmap for loop')
        print(configmap.metadata.name)
        configmapname = configmap.metadata.name
        if 'containerlist' in configmapname:
            configmaplist.append(configmapname)
        else:
            print(f'Configmapname {configmapname} is not relevant')
    return configmaplist

def get_configmap_data(configmap_name):
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    try:
        configmap = v1.read_namespaced_config_map(name=configmap_name, namespace='admin')
        data = configmap.data
        print(f"Key-Value pairs in ConfigMap '{configmap_name}' in namespace admin:")
        for key, value in data.items():
            configmap_key = key
            configmap_value = value

        print(f"Key: {configmap_key}, Value: {configmap_value}")

        return configmap_key, configmap_value
    except Exception as e:
        print(f"Error: {e}")
        return None

def apply_configmap(configmapname, configmapkey, configmapvalue):
    config.load_incluster_config()
    if 'current-yaml-commit-sha' in configmapname:
        configmapnamelabel = configmapname
    else:
        configmapnamelabel = 'containerlist-' + configmapname
    api_instance = client.CoreV1Api()
    config_map_manifest = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(
            namespace="admin",
            name=configmapnamelabel
        ),
        data={
            configmapkey: configmapvalue,
        }
    )

    namespace = "admin"

    try:     
        api_instance.create_namespaced_config_map(namespace, body=config_map_manifest)
        print(f"{configmapname} configmap created successfully.")

    except Exception as e:
        print(f"Error creating ConfigMap: {str(e)}")

def update_configmap(configmap_name, key, value):
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    data = {key : value}

    try:
        current_configmap = v1.read_namespaced_config_map(name=configmap_name, namespace='admin')
        current_configmap.data = data

        # Apply the changes
        updated_configmap = v1.patch_namespaced_config_map(
            name=configmap_name,
            namespace='admin',
            body=current_configmap
        )

        print(f"ConfigMap '{configmap_name}' in namespace admin updated successfully.")
        return updated_configmap
    except Exception as e:
        print(f"Error: {e}")
        return None


def delete_configmap(configmap_name):
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    try:
        v1.delete_namespaced_config_map(name=configmap_name, namespace='admin')
        print(f"ConfigMap '{configmap_name}' deleted successfully'.")
    except Exception as e:
        print(f"Error: {e}")

def get_namespaces():
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace_list = v1.list_namespace()
    # print('namespace_list is:', namespace_list)
    return namespace_list


def pods_status(item_name, item_namespace):
    print('Checking deployed pods')
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    status_counter = 0
    print('item namespace is: ', item_namespace)
    
    while True:
        pendingflag=False
        pod_list = v1.list_namespaced_pod(item_namespace)

        for pod in pod_list.items:
            if item_name in pod.metadata.name:
                pod_status = pod.status.phase
                print(f'pod name is {pod.metadata.name} and status is {pod_status}')

                if pod_status == "Pending" or pod_status == "ContainerCreating":
                    pendingflag=True
                    status_counter +=1
                    time.sleep(1)
                    print('There is a pod still being created')
                    break
                else:
                    print('No pending pods')

        if pendingflag != True or status_counter>=50:
            print('No pending states found')
            break


def find_corresponding_yaml(container_name):
    string_found = False
    string_to_find = str('app: ' + container_name)
    full_file_array = []
    for foldername, subfolders, filenames in os.walk(f'/{pvc_name}/{repo_name}/{folder_name}'):
        for filename in filenames:
            file_path = os.path.join(foldername, filename)
            full_file_array.append(file_path)
            print(file_path)

    for file_name in full_file_array:
        try:
            with open(file_name, 'r') as file:
                file_content = file.read()
                if string_to_find in file_content:
                    string_found = True
                    return file_name
        except Exception as e:
            print(f"Error reading file '{file_name}': {e}")
        
    if string_found == False:
        return 'no_yaml_found'

def container_versions():

    current_configmaps = get_configmaps()
    print('Relevant configmap list is ', current_configmaps)

    url = f"https://api.github.com/users/{user_account}/packages?package_type=container"

    token = os.getenv('package-checker-token')

    headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28"
    }
    try:
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)

        # Initialize an empty NumPy array
        package_name_array = np.array([])
        package_id_array = np.array([], dtype='int16')

        # Iterate over each entry and append values to the array
        for entry in json_data:
            print('Entry is: ', entry)
            name = entry['name']
            print('Name is', name)
            id = entry['id']
            if str(custom_container_prefix+'-') in str(name):
                print(f'Custom prefix found in {name}')
                package_name = str(name)[len(str(custom_container_prefix+"-")):]
            # Create a numpy array with the values for the current entry
                package_name = np.array([package_name])
                package_id = np.array([id])
            # Append the entry array to the result array
                package_name_array = np.append(package_name_array, package_name)
                package_id_array = np.append(package_id_array, package_id)
            elif "pipelineinitialisation" in name:
                package_name = np.array([name])
                package_id = np.array([id])
                package_name_array = np.append(package_name_array, package_name)
                package_id_array = np.append(package_id_array, package_id)
            else:
                print(f'{name} is not being used in this cluster')

    except Exception as e:
        print('Error with GitHub API when gathering list of container images')
    package_number = 0
    print('package name array is: ', package_name_array)

    for package_name in package_name_array:

        if "pipelineinitialisation" not in package_name:
            ghcr_image_name = custom_container_prefix+'-'+package_name
        else:
            ghcr_image_name = package_name
        
        url = "https://api.github.com/user/packages/container/" + str(ghcr_image_name) + "/versions"

        headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28"
        }

        try:
            response = requests.request("GET", url, headers=headers)
            json_data = json.loads(response.text)

            sha_value = next((item['name'] for item in json_data if 'name' in item), None)

            image_url = f'ghcr.io/{user_account}/' + str(ghcr_image_name) + ':main@' + str(sha_value)

            print('package id is :', package_id_array[package_number])
            print('package_name is :', package_name)
            print('sha_value is :', sha_value)
            print("image_url is: ", image_url)

            if str('containerlist-'+package_name) in current_configmaps: 
                print('Existing configmap')
                stored_configmaps_values = get_configmap_data('containerlist-'+package_name)
                stored_configmap_key = str(stored_configmaps_values[0])
                stored_configmap_value = str(stored_configmaps_values[1])
                if str(package_id_array[package_number])!=stored_configmap_key or str(image_url)!=stored_configmap_value:
                    print('Updating Configmap')
                    update_configmap(str('containerlist-'+package_name), str(package_id_array[package_number]), str(image_url))
                    yaml_file = find_corresponding_yaml(package_name)
                    job_name = str(''.join(random.choices(string.ascii_lowercase, k=5)) + '-' + package_name)
                    if yaml_file!='no_yaml_found':
                        print('Applying new package version') #NEED TO CHANGE SO THAT IT DELETES OLD POD FIRST
                        delete_and_deploy_flag = True
                        with open(str('/' + yaml_file), 'r') as file:
                            try:
                                yaml_data = yaml.safe_load(file)
                                yaml_kind = yaml_data.get('kind')
                                item_name = yaml_data.get('metadata', {}).get('name', None)
                                item_namespace = yaml_data.get('metadata', {}).get('namespace', None)
                            except ComposerError as e:
                                    print(f"YAML parsing error: {e}. Remove --- or multiple yaml components")
                                    break
                            except Exception as e:
                                print(f"An unexpected error occurred: {e}")
                                break
                        runyaml(job_name, str(image_url), yaml_file, 'apply', delete_and_deploy_flag, yaml_kind, item_name, item_namespace) #this will only run for changes
                    else:
                        print('No corresponding yaml file found for ', package_name)
                else:
                    print('Package configuration unchanged')
            else: #new packages need adding
                apply_configmap(str(package_name), str(package_id_array[package_number]), str(image_url))
                #only apply once yaml file has been put in place so won't be run from here
        except Exception as e:
            print('Error with GitHub API when getting details of package')
        package_number+=1

    # check old configmap names and delete if package if no longer there
    # hasn't been fully tested as would need to delete package from ghcr
    for configmapitem in current_configmaps:
        print('Checking configmap item: ', configmapitem)
        still_in_array = False
        configmapitem_name = str(configmapitem).split('-')[1]
        for name_value in package_name_array:
            #checking to see if old packages can be deleted from configmap
            if configmapitem_name in name_value:
                still_in_array = True
                break
            else:
                print('Going through other options')

        if still_in_array == False:
            delete_configmap(configmapitem)
        else:
            print('Configmap still present')


def cloneyamlrepo(commit_hash):
    access_token = os.getenv('clone-yaml-token')
    repo_url = 'https://' + user_account + ':' + access_token + '@' + f'github.com/{user_account}/{repo_name}.git'
    local_dir = f'/{pvc_name}/{repo_name}' #pvc folder location
    print(local_dir)
    print('Here 1')
    if os.path.exists(local_dir):
        print('file location exists')
        repo = Repo(local_dir)
        repo.git.fetch()
    else:
        repo = Repo.clone_from(repo_url, local_dir) #on mac can't create folder. Works on pi
    print('Commit hash is : ', commit_hash)
    repo.git.checkout(commit_hash)
    print("Repository cloned successfully")


def runyaml(jobname, image_url_var, yaml_file, state, delete_and_deploy_flag, yaml_kind, item_name, item_namespace):
    pull_ghcr_image_token = os.getenv('pull-ghcr-image-token')
    config.load_incluster_config()
    api_instance = client.BatchV1Api()

    job_manifest = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=jobname,
            namespace="admin",
        ),
        spec=client.V1JobSpec(
            backoff_limit=2,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    service_account_name="admin-sa",
                    containers=[
                        client.V1Container(
                            name="script",
                            image="bitnami/kubectl:latest",
                            command=["/bin/bash", "-c", f'cat {yaml_file} | sed "s#pull_ghcr_image_token#{pull_ghcr_image_token}#g" | sed "s#image_url_var#{image_url_var}#g" | if [[ "{delete_and_deploy_flag}" == "True" ]]; then echo "Delete first"; kubectl delete {yaml_kind} {item_name} -n {item_namespace}; echo "Deletion Occured"; kubectl apply -f -; echo "Redeploy complete"; else kubectl {state} -f -; fi'],
                            volume_mounts=[client.V1VolumeMount(mount_path=f"/{pvc_name}", name="yaml-files")]
                        )    
                    ],
                    volumes=[client.V1Volume(name="yaml-files", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="yamlstore"))],
                    restart_policy="Never"
                )
            )
        )
    )

    namespace = "admin"

    try:
        api_instance.create_namespaced_job(namespace, body=job_manifest)
        print("Job created successfully.")
    except Exception as e:
        print(f"Error creating Job: {str(e)}")
    with open(str('/' + yaml_file), 'r') as file:
        try:
            yaml_data = yaml.safe_load(file)
            item_name = yaml_data.get('metadata', {}).get('name', None)
            item_namespace = yaml_data.get('metadata', {}).get('namespace', None)
            print(item_name)
            print(item_namespace)
            pods_status(item_name, item_namespace)
        except ComposerError as e:
                print(f"YAML parsing error: {e}. Remove --- or multiple yaml components")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        except MarkedYAMLError as e:
            print("YAML parsing error")

    print('Finished job')
    

# Function to count occurrences of the string "sha" in a list of dictionaries
def count_occurrences(data_list, target_string):
    count = 0
    for item in data_list:
        if isinstance(item, dict):
            for key in item.keys():
                if isinstance(key, str) and target_string in key:
                    count += 1
    return count

# Function to count occurrences of 'filename'
def count_filename_occurrences(data):
    count = 0
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "filename":
                count += 1
            elif isinstance(value, (dict, list)):
                count += count_filename_occurrences(value)
    elif isinstance(data, list):
        for item in data:
            count += count_filename_occurrences(item)
    return count

def status_check_and_run(file_status, directory, deploy_after_clone):
    last_hyphen_index = directory.rfind('-')
    if last_hyphen_index != -1:
        job_name = directory[last_hyphen_index + 1: -4]  # -4 to exclude '.yml'
        print(job_name)
    else:
        print("No hyphen found.")
        job_name = ''.join(random.choices(string.ascii_letters, k=5))
    
    yaml_file_name_link = f'{pvc_name}/{repo_name}/{folder_name}/' + directory
    print('yaml_file_name_link is : ', yaml_file_name_link)

    if file_status in {"added"}:
        print('added')
        deploy_after_clone.append(directory)

    elif file_status=="removed":
        if "pipelineinitialisation" not in directory:
            state='delete'
            image_url_var_str = 'not_needed' #only needed when doing apply. Image ref not needed for delete, as works on type and name
            delete_and_deploy_flag = False
            yaml_kind = "empty"
            item_name = "empty"
            item_namespace = "empty"
            runyaml(job_name, image_url_var_str, yaml_file_name_link, state, delete_and_deploy_flag, yaml_kind, item_name, item_namespace)
            print('removed')
        else:
            print("Tried to delete pipeline initialisation process. Please add this file back into repo: ", directory)
        
    elif file_status in {"modified", "changed"}:
        print('modified')
        deploy_after_clone.append(directory)

    elif file_status in {"renamed", "copied", "unchanged"}:
        print(f'File status {file_status} not supported')


def extract_volumes(yaml_data):
    volumes = []
    if isinstance(yaml_data, dict):
        if 'volumes' in yaml_data:
            volumes.extend(yaml_data['volumes'])
        for value in yaml_data.values():
            volumes.extend(extract_volumes(value))
    elif isinstance(yaml_data, list):
        for item in yaml_data:
            volumes.extend(extract_volumes(item))
    return volumes

def extract_spec(yaml_data):
    specs = []
    if isinstance(yaml_data, dict):
        yaml_data = yaml_data.get('template', {})
        if 'spec' in yaml_data:
            specs.extend(yaml_data['spec'])
        for value in yaml_data.values():
            specs.extend(extract_spec(value))
    elif isinstance(yaml_data, list):
        for item in yaml_data:
            specs.extend(extract_spec(item))
    return specs

def yamlcommitsha():

    # try to read configmap for commit sha. Mark as empty if cannot be found.
    previous_commit_sha_key_value = get_configmap_data('current-yaml-commit-sha')
    if previous_commit_sha_key_value == None:
        previous_commit_sha = apply_configmap('current-yaml-commit-sha', 'yaml-sha', 'empty')
        print('Created initial previous commit sha')
    else:
        print('Configmap already created')

    url = f"https://api.github.com/repos/{user_account}/{repo_name}/commits"

    token = os.getenv('yaml-commit-checker-token')

    headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28"
    }
    try:
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)

        # Count occurrences of the string "sha"
        sha_count = count_occurrences(json_data, "sha")
        print('Count is: ', sha_count)

        new_commit_sha_array = np.array([])
        new_commit_count = 0

        previous_commit_sha_key_value = get_configmap_data('current-yaml-commit-sha')
        print(previous_commit_sha_key_value[1])
        if previous_commit_sha_key_value[1] == 'empty':
            print('This is the first time and only use latest commit')
            use_latest_commit = True
            previous_commit_sha = 'empty'
        else:
            previous_commit_sha = str(previous_commit_sha_key_value[1])
            use_latest_commit = False

    except Exception as e:
        print('Error with GitHub API when trying to receive commit history')
        new_commit_count = 0
        sha_count = 0
        use_latest_commit = False

    if sha_count!=0:            
        for commit_number in range(0,sha_count):
            commit_sha = json_data[commit_number]["sha"]
            if commit_sha!=previous_commit_sha:
                new_commit_sha = np.array([commit_sha])
                new_commit_sha_array = np.append(new_commit_sha_array, new_commit_sha)
                new_commit_count += 1
            else:
                print('Not a new commit')
                break

        #this will run when no previous commit sha is detected
        if use_latest_commit == True:
            print('Only using latest commit')
            flipped_new_commit_sha_array = np.array((new_commit_sha_array[0]))
            flipped_new_commit_sha_array = flipped_new_commit_sha_array.reshape(1)
            print('new_commit_sha_array is', flipped_new_commit_sha_array)
            cloneyamlrepo(str(new_commit_sha_array[0])) #clone repo

            #=================================================================================
            # sort folders
            directory = f'/{pvc_name}/{repo_name}/{folder_name}'

            folders = [f for f in os.listdir(directory) if os.path.isdir(os.path.join(directory, f))]
            print('folders are: ', folders)
            folder_list_details = []

            for folder in folders:
                print(folder)
                try:
                    order, command_foldername = folder.split('-')
                    combined_array = np.array([int(order), folder])
                    folder_list_details.append(combined_array)
                except ValueError:
                    print('File name did not contain a dash')

            # Stack the arrays vertically
            folder_list_separated = np.vstack(folder_list_details)
            print('folder_list_separated: ', folder_list_separated)
            # Convert the first column to integers for sorting
            first_folder_column = folder_list_separated[:, 0].astype(int)

            # Sort the indices based on the first column
            sorted_folder_indices = np.argsort(first_folder_column)

            # Apply the sorted indices to the array
            sorted_folder_list = folder_list_separated[sorted_folder_indices]

            # print('sorted_folder_list :', sorted_folder_list)
            print('folder list has been sorted')

            #=================================================================================
            #sort files
            for folder_row in sorted_folder_list:
                print('folder row is :', folder_row)
                directory = f'/{pvc_name}/{repo_name}/{folder_name}/'+str(folder_row[1])
                file_list = os.listdir(directory)
                file_list_details = []
                print('file_list :', file_list)

                for file in file_list:
                    try:
                        order, command_filename = file.split('-')
                        combined_array = np.array([int(order), file])
                        file_list_details.append(combined_array)
                    except ValueError:
                        print('File name did not contain a dash')

                # Stack the arrays vertically
                file_list_separated = np.vstack(file_list_details)
                print('file_list_separated: ', file_list_separated)
                # Convert the first column to integers for sorting
                first_file_column = file_list_separated[:, 0].astype(int)

                # Sort the indices based on the first column
                sorted_file_indices = np.argsort(first_file_column)

                # Apply the sorted indices to the array
                sorted_file_list = file_list_separated[sorted_file_indices]

                print('sorted_file_list :', sorted_file_list)

                for row in sorted_file_list:
                    yaml_file_name = row[1]
                    job_name = str(os.path.splitext(yaml_file_name)[0])
                    print('Job name is: ', job_name)
                    yaml_file_name_link = f'{pvc_name}/{repo_name}/{folder_name}/' + str(folder_row[1]) + '/' + str(yaml_file_name)
                    print('yaml_file_name_link is : ', yaml_file_name_link)

                    # does yaml file name (main bit) match any of the container names?
                    configmaplist = get_configmaps()
                    for package_name_value in configmaplist:
                        package_name_value_strip = str(package_name_value).split('-')[1]
                        if package_name_value_strip in job_name:
                            key_and_value = get_configmap_data(package_name_value)
                            image_url_var_str = key_and_value[1]
                            print('Importing custom image url')
                            print('Image url is', image_url_var_str)
                            break
                        else:
                            image_url_var_str="default"
                            print('Standard package deployment')

                    delete_and_deploy_flag = False
                    yaml_kind = "empty"
                    item_name = "empty"
                    item_namespace = "empty"
                    runyaml(job_name, image_url_var_str, yaml_file_name_link, 'apply', delete_and_deploy_flag, yaml_kind, item_name, item_namespace)
                    print(f"Applying {yaml_file_name}")
                    # time.sleep(10) #I don't like this, but would have to pull info from yaml file to query pod status. Future enhancement
            update_configmap('current-yaml-commit-sha', 'yaml-sha', str(new_commit_sha_array[0]))
            print('Initial configmap for yaml sha applied')

    #this will run when previous commit sha is detected
        else:
            print('Using all new commits')
            flipped_new_commit_sha_array = np.flipud(new_commit_sha_array) #so that we're executing the oldest changes first
            print('flipped_new_commit_sha_array is: ', flipped_new_commit_sha_array)
            print('new_commit_count is: ', new_commit_count)

            for new_commit_reference in flipped_new_commit_sha_array:
                print('new_commit_reference is :', new_commit_reference)
                url = str(f"https://api.github.com/repos/{user_account}/{repo_name}/commits/" + new_commit_reference)

                headers = {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28"
                }

                response = requests.request("GET", url, headers=headers)
                json_data = json.loads(response.text)

                # Count occurrences of 'filename'
                filename_count = count_filename_occurrences(json_data)

                print(f'The number of occurrences of "filename" is: {filename_count}')
                # filename_and_status_array = np.array([])
                filename_and_status_array_count = 0

                for filename_number in range(0, filename_count):
                    directory = json_data.get("files", [{}])[filename_number].get("filename", None)
                    print('directory is: ', directory)
                    if '/' in directory:
                        directory = directory[len(f'{folder_name}/'):]
                        split_directory = directory.split('/')
                        
                        folder_na = split_directory[0]
                        if '-' in folder_na:
                            split_folder_name = folder_na.split('-')
                            folder_number = split_folder_name[0]
                            only_folder_name = split_folder_name[1]

                            file_name = split_directory[1]
                            if '-' in file_name:
                                split_file_name = file_name.split('-')
                                file_number = split_file_name[0]
                                only_file_name = split_file_name[1]

                                filename_status = json_data.get("files", [{}])[filename_number].get("status", None)
                                filename_and_status = np.array((folder_number, only_folder_name, file_number, only_file_name, filename_status, directory))
                                if filename_and_status_array_count==0:
                                    filename_and_status_array = filename_and_status
                                    filename_and_status_array_count+=1
                                else:
                                    filename_and_status_array = np.vstack((filename_and_status_array, filename_and_status))
                            else:
                                print(f'file name in {directory} not in correct format')
                        else:
                            print(f'folder name in {directory} not in correct format')
                    else:
                        print(f'No / in directory {directory} and so not included')
                    
                if 'filename_and_status_array' not in locals():
                    filename_and_status_array = np.empty((0, 6), dtype=str)
                else:
                    print('Array already created')
                    print(filename_and_status_array)
                    deploy_after_clone = []
                    if filename_and_status_array.ndim <= 1:
                        filesorted_filename_and_status_array = filename_and_status_array
                        file_status = filesorted_filename_and_status_array[4]
                        directory = filesorted_filename_and_status_array[5]
                        status_check_and_run(file_status, directory, deploy_after_clone)
                    else:
                        folder_number_column = filename_and_status_array[:, 0]
                        sort_folder_number_column = np.argsort(folder_number_column)
                        foldersorted_filename_and_status_array = filename_and_status_array[sort_folder_number_column]
                        # print(foldersorted_filename_and_status_array)

                        sort_by_file_number = np.lexsort((foldersorted_filename_and_status_array[:, 2].astype(int), foldersorted_filename_and_status_array[:, 0].astype(int)))
                        filesorted_filename_and_status_array  = foldersorted_filename_and_status_array[sort_by_file_number]
                        print(filesorted_filename_and_status_array)

                        # find number of rows in above array
                        num_files_to_handle = filesorted_filename_and_status_array.shape[0]

                        for changed_file_num in range(0, num_files_to_handle):
                            file_status = filesorted_filename_and_status_array[changed_file_num, 4]
                            directory = filesorted_filename_and_status_array[changed_file_num, 5]
                            status_check_and_run(file_status, directory, deploy_after_clone)
                    
                    print("deploy_after_clone array is: ", deploy_after_clone)
                    print("new_commit_reference is: ", new_commit_reference)
                
                    print('Cloning new repo')
                    cloneyamlrepo(str(new_commit_reference))
                    
                    update_configmap('current-yaml-commit-sha', 'yaml-sha', new_commit_reference)

                
                    for file_to_deploy in deploy_after_clone:
                        print(file_to_deploy)
                        last_hyphen_index = file_to_deploy.rfind('-')
                        if last_hyphen_index != -1:
                            job_name = str(''.join(random.choices(string.ascii_lowercase, k=5)) + '-' + file_to_deploy[last_hyphen_index + 1: -4])  # -4 to exclude '.yml' , got to add random letters as otherwise job name is repeated
                            print(job_name)
                        else:
                            print("No hyphen found.")
                            job_name = ''.join(random.choices(string.ascii_lowercase, k=5))
                        yaml_file_name_link = f'{pvc_name}/{repo_name}/{folder_name}/' + str(file_to_deploy)
                        print('yaml_file_name_link is : ', yaml_file_name_link)
                        
                        # does yaml file name (main bit) match any of the container names?
                        configmaplist = get_configmaps()
                        for package_name_value in configmaplist:
                            package_name_value_strip = str(package_name_value).split('-')[1]
                            if package_name_value_strip in job_name:
                                key_and_value = get_configmap_data(package_name_value)
                                image_url_var_str = key_and_value[1]
                                print('Importing custom image url')
                                print('Image url is', image_url_var_str)
                                break
                            else:
                                image_url_var_str="default"
                                print('Standard package deployment')

                        state='apply'
                        if "pod" or "job" or "deployment" or "cronjob" or "replicaset" or  "statefulset" or  "daemonset" in job_name:
                            delete_and_deploy_flag = True
                            print(f'Determining kinds and names for {job_name}')
                            with open(str('/' + yaml_file_name_link), 'r') as file:
                                try:
                                    yaml_data = yaml.safe_load(file)
                                    item_name = yaml_data.get('metadata', {}).get('name', None)
                                    item_namespace = yaml_data.get('metadata', {}).get('namespace', None)
                                except ComposerError as e:
                                    print(f"YAML parsing error: {e}. Remove --- or multiple yaml components")
                                    break
                                except Exception as e:
                                    print(f"An unexpected error occurred: {e}")
                                    break
                                except MarkedYAMLError as e:
                                    break
                                    print("YAML parsing error")
                            if "pod" in job_name:
                                yaml_kind = "pod"
                            elif "job" in job_name:
                                yaml_kind = "job"
                            elif "deployment" in job_name:
                                yaml_kind = "deployment"
                            elif "cronjob" in job_name:
                                yaml_kind = "cronjob"
                            elif "replicaset" in job_name:
                                yaml_kind = "replicaset"
                            elif "statefulset" in job_name:
                                yaml_kind = "statefulset"
                            else:
                                yaml_kind = "daemonset"
                            
                        else:
                            delete_and_deploy_flag = False
                        print(yaml_kind, item_name, item_namespace)
                        runyaml(job_name, image_url_var_str, yaml_file_name_link, state, delete_and_deploy_flag, yaml_kind, item_name, item_namespace)
                        print(f"Applying {file_to_deploy}")

                        if "configmap" or "secret" in job_name:
                            print('Updating corresponding app for configmap/secret')
                            with open(str('/' + yaml_file_name_link), 'r') as file:
                                try:
                                    yaml_data = yaml.safe_load(file)
                                    item_name = yaml_data.get('metadata', {}).get('name', None)
                                except ComposerError as e:
                                    print(f"YAML parsing error: {e}. Remove --- or multiple yaml components")
                                    break
                                except Exception as e:
                                    print(f"An unexpected error occurred: {e}")
                                except MarkedYAMLError as e:
                                    print("YAML parsing error")

                                file_in_search_array = []
                                for foldername, subfolders, filenames in os.walk(f'/{pvc_name}/{repo_name}/{folder_name}'):
                                    print('Searching files for app deploy file')
                                    for filename in filenames:
                                        file_path = os.path.join(foldername, filename)
                                        if "pod" or "job" or "deployment" or "cronjob" or "replicaset" or  "statefulset" or  "daemonset" in file_path:
                                            file_in_search_array.append(file_path)
                                        else:
                                            print('Not image linked file')
                            print('file_in_search_array', file_in_search_array)
                                    
                            # array of all files that contain pod/job/deployment name
                            for file_in_search in file_in_search_array:
                                print('Searching for corresponding app in', file_in_search)
                                with open(str(file_in_search), 'r') as file: # open every file in search of configmap name
                                    try:
                                        yaml_data = yaml.safe_load(file)
                                        if "configmap" in job_name:
                                            print('If configmap then...')
                                            volumes = extract_volumes(yaml_data)
                                            print('volumes are:', volumes)
                                            for volume in volumes:
                                                print('volume is: ', volume)
                                                print(type(volume))
                                                print('Looking at individual volumes')
                                                if 'configMap' in volume:
                                                    print('configMap in volume')
                                                    dict_part = volume.get('configMap', {})
                                                    print('dict part is: ', dict_part)
                                                    config_map = dict_part.get('name', '')
                                                    print('printing configmap', config_map)
                                                    if str(config_map) == item_name:
                                                        print('Configmap match found')
                                                        delete_and_deploy_flag = True
                                                        yaml_kind = yaml_data.get('kind')
                                                        item_name = yaml_data.get('metadata', {}).get('name', None)
                                                        item_namespace = yaml_data.get('metadata', {}).get('namespace', None)
                                                        last_hyphen_index = file_in_search.rfind('-')
                                                        if last_hyphen_index != -1:
                                                            job_name = str(''.join(random.choices(string.ascii_lowercase, k=5)) + '-' + file_in_search[last_hyphen_index + 1: -4])  # -4 to exclude '.yml' , got to add random letters as otherwise job name is repeated
                                                            print(job_name)
                                                        runyaml(job_name, image_url_var_str, file_in_search, state, delete_and_deploy_flag, yaml_kind, item_name, item_namespace)
                                                    else:
                                                        print('Searching next volume')
                                        if "secret" in job_name:
                                            print('If configmap then...')
                                            volumes = extract_volumes(yaml_data)
                                            specs = extract_spec(yaml_data)
                                            print('volumes are:', volumes)
                                            for volume in volumes:
                                                print('volume is: ', volume)
                                                print(type(volume))
                                                print('Looking at individual volumes')
                                                if 'secret' in volume:
                                                    print('secret in volume')
                                                    dict_part = volume.get('secret', {})
                                                    print('dict part is: ', dict_part)
                                                    secret_name = dict_part.get('secretName', '')
                                                    print('printing configmap', secret_name)
                                                    if str(secret_name) == item_name:
                                                        print('Secret match found')
                                                        delete_and_deploy_flag = True
                                                        yaml_kind = yaml_data.get('kind')
                                                        item_name = yaml_data.get('metadata', {}).get('name', None)
                                                        item_namespace = yaml_data.get('metadata', {}).get('namespace', None)
                                                        last_hyphen_index = file_in_search.rfind('-')
                                                        if last_hyphen_index != -1:
                                                            job_name = str(''.join(random.choices(string.ascii_lowercase, k=5)) + '-' + file_in_search[last_hyphen_index + 1: -4])  # -4 to exclude '.yml' , got to add random letters as otherwise job name is repeated
                                                            print(job_name)
                                                        runyaml(job_name, image_url_var_str, file_in_search, state, delete_and_deploy_flag, yaml_kind, item_name, item_namespace)
                                                    else:
                                                        print('Searching next volume')
                                                elif 'imagePullSecrets' in volume:
                                                    print('secret in volume')
                                                    for spec in specs:
                                                        dict_part = spec.get('imagePullSecrets', {})
                                                        print('dict part is: ', dict_part)
                                                        secret_name = dict_part.get('name', '')
                                                        print('printing configmap', secret_name)
                                                        if str(secret_name) == item_name:
                                                            print('Secret match found')
                                                            delete_and_deploy_flag = True
                                                            yaml_kind = yaml_data.get('kind')
                                                            item_name = yaml_data.get('metadata', {}).get('name', None)
                                                            item_namespace = yaml_data.get('metadata', {}).get('namespace', None)
                                                            last_hyphen_index = file_in_search.rfind('-')
                                                            if last_hyphen_index != -1:
                                                                job_name = str(''.join(random.choices(string.ascii_lowercase, k=5)) + '-' + file_in_search[last_hyphen_index + 1: -4])  # -4 to exclude '.yml' , got to add random letters as otherwise job name is repeated
                                                                print(job_name)
                                                            runyaml(job_name, image_url_var_str, file_in_search, state, delete_and_deploy_flag, yaml_kind, item_name, item_namespace)
                                                        else:
                                                            print('Searching next volume')
                                    except ComposerError as e:
                                        print(f"YAML parsing error: {e}. Remove --- or multiple yaml components")
                                    except Exception as e:
                                        print(f"An unexpected error occurred: {e}")
                                    except MarkedYAMLError as e:
                                        print("YAML parsing error")
                               
refresh_counter = 0     

while True:
    container_versions()   
    yamlcommitsha()
    refresh_counter+=1
    print('refresh_counter = ', refresh_counter)
    time.sleep(20)               

# this last bit would need to be adapted if it were changed to a cronjob.
