import msal
from azure.storage.filedatalake import DataLakeServiceClient

def list_directory_contents(filesystem_client):
    """Gets a list of all files in a directory (Azure storage)"""

    # List directory contents
    paths = filesystem_client.get_paths()

    if paths:
        logging.info("There are files in the folder")  # , len(paths))
        return paths
    else:
        logging.info("There are no files in the directory.")
        
def fetch_from_lake(filesystem_client, file_path):
    """Connects to Azure storage and retrieves multiple json files"""

    directory = file_path.rsplit('/', 1)[0]
    name = file_path.rsplit('/', 1)[1]

    directory_client = filesystem_client.get_directory_client(directory)

    # local_file = open("C:\\file-to-download.txt",'wb')

    file_client = directory_client.get_file_client(name)
    # Retrieve the json and turn it into a pandas data frame
    streamdownloader = file_client.download_file()
    file_reader = pd.read_json(streamdownloader.readall())
    file_reader = file_reader.get('value')
    file_reader = file_reader.to_frame()
    file_reader = file_reader['value'].apply(pd.Series)

    logging.info("Json retrieved from %s", file_path)
    return file_reader
 
def upload_to_datalake(filesystem_client, file_path, directory, data):
    """Connects to Azure storage and uploads a file to a data lake directory"""
    directory_client = filesystem_client.get_directory_client(directory)
    file_client = directory_client.get_file_client(file_path)
    file_client.create_file()
    file_client.upload_data(data, overwrite=True)
    # data is only committed when flush is called
    file_client.flush_data(len(data.encode()))

    logging.info("Json or CSV saving to %s", file_path)
    
def initialize_datalake_client(connection_str=CONNECTSTR, file_system='landing-zone'):
    """Creates a service client for the Azure data lake"""

    service_client = DataLakeServiceClient.from_connection_string(connection_str)

    # create the filesystem
    filesystem_client = service_client.get_file_system_client(file_system)
    logging.info("Successfully connected to Azure Data Lake")
    return filesystem_client
  
  
  def get_ad_token(app_id, tenant_id, client_secret):
    """Fetches a fresh token from Active Directory"""

    scope = 'https://management.core.windows.net/.default'
    authority_url = 'https://login.microsoftonline.com/' + tenant_id

    # Create a preferably long-lived app instance which maintains a token cache
    app = msal.ConfidentialClientApplication(
        app_id,
        authority=authority_url,
        client_credential=client_secret)

    # The pattern to acquire a token looks like this.
    result = None

    # Firstly, looks up a token from cache
    result = app.acquire_token_silent([scope], account=None)

    if not result:
        logging.info("No suitable token exists in cache. Let's get a new one from AAD.")
        result = app.acquire_token_for_client(scopes=scope)

    if "access_token" in result:
        return result["access_token"]

    else:
        logging.error((result.get("error")))
        logging.error((result.get("error_description")))
        logging.error((result.get("correlation_id")))

