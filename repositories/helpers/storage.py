import basedosdados as bd
from pathlib import Path

class StoragePlus(bd.Storage):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def download(
        self,
        filename,
        file_path,
        partitions,
        mode="raw",
        if_exists="raise"
    ):
        """ Download a single file from storage from <bucket_name>/<mode>/<dataset_id>/<table_id>/<partitions>/<filename> 
        
        There are 2 modes:

        * `raw`: download file from raw mode
        * `staging`: download file from staging mode

        Args:
            filename (str): File to download

            mode (str): Folder of which dataset to download [raw|staging]

            partitions (str, pathlib.PosixPath, or dict): Optional.
                    *If adding a single file*, use this to add it to a specific partition.
                    * str : `<key>=<value>/<key2>=<value2>`
                    * dict: `dict(key=value, key2=value2)`

            if_exists (str): Optional.
                What to do if data exists
                * 'raise' : Raises Conflict exception
                * 'replace' : Replace table
                * 'pass' : Do nothing
        """
        if (self.dataset_id is None) or (self.table_id is None):
            raise Exception("You need to pass dataset_id and table_id")

        self._check_mode(mode)

        # Create blob path
        blob_name = self._build_blob_name(filename, mode, partitions)
        blob = self.bucket.blob(blob_name)

        # Create local file path
        _file_path = f"{file_path}/{filename}"

        # Download
        if (not Path(file_path).is_file()) or (Path(file_path).is_file() and if_exists == 'replace'):
            print(f"deleting file {file_path}")
            Path(file_path).unlink(missing_ok=True)
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(file_path)
        elif if_exists == "pass":
            pass
        else:
            raise Exception(
                        f"Data already exists at {_file_path}. "
                        "Set if_exists to 'replace' to overwrite data"
                    )