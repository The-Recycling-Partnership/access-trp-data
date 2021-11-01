"""Functions to upload and download data from TRP's cloud storage
"""

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobClient,BlobServiceClient


def upload_data_to_azure_container(local_path: str, upload_name: str, container: str,
                                    sas_string: str=None, sas_string_src: str=None,
                                    overwrite: bool=False):
    """Uploads data to a TRP Azure storage container

    Note - you must provide either a SAS URL string or a path to a file containing a SAS
    URL string for this function to work

    Parameters
    ----------
    local_path : str
        relative path to file for upload
    upload_name : str
        the name that the file will have in Azure Storage
    container : str
        the name of the container that you want to upload your file to
    sas_string : str, optional
        a sas_url string, by default None
    sas_string_src : str, optional
        relative path to a file containing the SAS URL string, by default None
    overwrite : bool, optional
        whether or not to overwrite a file if it already exists in the container,
        by default False
    """
    if sas_string:
        connection_string = sas_string
    elif sas_string_src:
        with open(sas_string_src, "r") as f:
            connection_string = f.readline()
    else:
        raise Exception(
            """You must either pass a variable containing a SAS string to the argument `sas_string`
           or pass the relative file path to a file containing a SAS string to the argument `sas_string_src`
           in order to write data to Azure Storage.
            """
        )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container, blob=upload_name)
    try:
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data=data, overwrite=False)
            print(f"Successfully uploaded {local_path} to {upload_name}")
    except ResourceExistsError as e:
        if overwrite:
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data=data, overwrite=overwrite)
            print(f"Successfully overwrote {upload_name} with {local_path}")
        else:
            print("ResourceExistsError occurred")
            print(f"A file named {upload_name} already exists in container '{container}'")
            print("If you would like to replace the file, re-run `upload_data_to_azure()` with `overwrite=True`")
            raise e


def download_data_from_azure_container(item_name: str, download_location: str, container: str,
                                        sas_string: str=None, sas_string_src: str=None):
    """Downloads a file from a TRP Azure storage container

    Note - you must provide either a SAS URL string or a path to a file containing a SAS
    URL string for this function to work

    Parameters
    ----------
    item_name : str
        The name of the item as it appears in the Azure Container
    download_location : str
        The relative file path where you would like to save the item
    container : str
        the name of the container that you want to upload your file to
    sas_string : str, optional
        a sas_url string, by default None
    sas_string_src : str, optional
        relative path to a file containing the SAS URL string, by default None
    """
    if sas_string:
        connection_string = sas_string
    elif sas_string_src:
        with open(sas_string_src, "r") as f:
            connection_string = f.readline()
    else:
        raise Exception(
            """You must either pass a variable containing a SAS string to the argument `sas_string`
           or pass the relative file path to a file containing a SAS string to the argument `sas_string_src`
           in order to read data from Azure Storage.
            """
        )
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container, blob=item_name)
    try:
        with open(download_location, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
            print(f"Successfully downloaded file to {download_location}")
    except Exception as e:
        print(e)


def download_specific_blob(download_location: str, sas_url_string: str=None, sas_url_string_src: str=None):
    """Downloads a file specified by a blob SAS URL

    Note - you must provide either a blob SAS URL string or a path to a file containing a blob
    SAS URL string for this function to work. A blob SAS URL is different from a container SAS URL.

    Parameters
    ----------
    download_location : str
        The relative file path where you would like to save the item
    container : str
        the name of the container that you want to upload your file to
    sas_string : str, optional
        a sas_url string, by default None
    sas_string_src : str, optional
        relative path to a file containing the SAS URL string, by default None
    """
    if sas_url_string:
        connection_string = sas_url_string
    elif sas_url_string_src:
        with open(sas_url_string_src, "r") as f:
            connection_string = f.readline()
    else:
        raise Exception(
            """You must either pass a variable containing a SAS url string (not the same as a SAS string)
            to the argument `sas_url_string` or pass the relative file path to a file containing a SAS url
            string to the argument `sas_url_string_src` in order to download this specific blob from
            Azure Storage.
            """
        )
    blob_client = BlobClient.from_blob_url(connection_string)
    try:
        with open(download_location, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
            print(f"Successfully downloaded file to {download_location}")
    except Exception as e:
        print(e)