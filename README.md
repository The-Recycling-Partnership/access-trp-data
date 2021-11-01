# access-trp-data
Repo with demo functions for accessing data from TRP's Azure storage

# Examples

**Downloading a specific blob that you have been given direct access to**

```python
download_outfile = f"{local_dir}/data.csv"
sas_url_string_location = "not_secure.txt"

download_specific_blob(download_location=download_outfile,
							          sas_url_string_src=sas_url_string_location)```
                        
```python
download_outfile = f"{local_dir}/data.csv"
sas_url_string = os.getenv("SAS_URL")

download_specific_blob(download_location=download_outfile,
							          sas_url_string=SAS_URL)```
                        
                        
**Download contents of a container that you have been given access to**

```python
file_to_download = "data.csv"
download_outfile = f"{local_dir}/data.csv"
sas_url_string_location = "not_secure.txt"

download_data_from_azure_container(item_name=file_to_download, 
                                    download_location=download_outfile,
										                sas_string_src=container_sas_outfile)
```

```python
file_to_download = "data.csv"
download_outfile = f"{local_dir}/data.csv"
sas_url_string = os.getenv("SAS_URL")

download_data_from_azure_container(item_name=file_to_download,
										                download_location=download_outfile,
										                sas_string=sas_url_string)
```

