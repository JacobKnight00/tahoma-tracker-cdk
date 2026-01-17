"""
S3 utilities for uploading trained models and metadata.
"""
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class S3ModelUploader:
    """Handles uploading trained models and metadata to S3."""
    
    def __init__(self, bucket: str, region: str = 'us-west-2', profile: Optional[str] = None):
        """
        Initialize S3 uploader.
        
        Args:
            bucket: S3 bucket name
            region: AWS region
            profile: AWS profile name (optional)
        """
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.s3_client = session.client('s3', region_name=region)
        self.bucket = bucket
        logger.info(f"Initialized S3ModelUploader for bucket: {bucket}")
    
    def upload_model(
        self,
        model_path: Path,
        metadata_path: Path,
        s3_prefix: str,
        model_version: int,
        model_filename: Optional[str] = None,
        metadata_filename: Optional[str] = None
    ) -> dict:
        """
        Upload model and metadata to S3.
        
        Args:
            model_path: Local path to ONNX model file
            metadata_path: Local path to metadata JSON file
            s3_prefix: S3 prefix for storage (e.g., "models/v1")
            model_version: Version number of the model
            model_filename: Optional filename override for the uploaded model
            metadata_filename: Optional filename override for the uploaded metadata
        
        Returns:
            Dictionary with S3 paths of uploaded files (includes model_data if present)
        
        Raises:
            ClientError if upload fails
        """
        try:
            s3_paths = {}
            
            # Upload ONNX model
            model_key = f"{s3_prefix}/{model_filename or Path(model_path).name}"
            logger.info(f"Uploading model to s3://{self.bucket}/{model_key}")
            self.s3_client.upload_file(
                str(model_path),
                self.bucket,
                model_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )
            s3_paths['model'] = model_key
            logger.info(f"Successfully uploaded model to {model_key}")

            # Upload external data file if present
            data_path = model_path.with_suffix(model_path.suffix + ".data")
            if data_path.exists():
                data_key = f"{s3_prefix}/{(model_filename or Path(model_path).name)}.data"
                logger.info(f"Uploading external data file to s3://{self.bucket}/{data_key}")
                self.s3_client.upload_file(
                    str(data_path),
                    self.bucket,
                    data_key,
                    ExtraArgs={'ContentType': 'application/octet-stream'}
                )
                s3_paths['model_data'] = data_key
                logger.info(f"Successfully uploaded external data file to {data_key}")
            
            # Upload metadata
            metadata_key = f"{s3_prefix}/{metadata_filename or Path(metadata_path).name}"
            logger.info(f"Uploading metadata to s3://{self.bucket}/{metadata_key}")
            self.s3_client.upload_file(
                str(metadata_path),
                self.bucket,
                metadata_key,
                ExtraArgs={'ContentType': 'application/json'}
            )
            s3_paths['metadata'] = metadata_key
            logger.info(f"Successfully uploaded metadata to {metadata_key}")
            
            # Create/update version tracking metadata
            self._create_version_manifest(s3_prefix, model_version)
            
            return s3_paths
        
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
    
    def _create_version_manifest(self, s3_prefix: str, model_version: int) -> None:
        """
        Create a version manifest at the root of models/ to track deployed versions.
        
        Args:
            s3_prefix: S3 prefix (e.g., "models/v1")
            model_version: Version number
        """
        try:
            manifest_key = "models/versions.json"
            
            # Try to read existing manifest
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=manifest_key)
                manifest = json.load(response['Body'])
            except ClientError:
                manifest = {'versions': []}
            
            # Add new version if not already present
            version_entry = {
                'version': model_version,
                'timestamp': datetime.utcnow().isoformat() + "Z",
                's3_prefix': s3_prefix
            }
            
            # Update only if this version not already in manifest
            if not any(v['version'] == model_version for v in manifest.get('versions', [])):
                manifest['versions'].append(version_entry)
                manifest['latest_version'] = model_version
                
                # Upload updated manifest
                manifest_data = json.dumps(manifest, indent=2)
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=manifest_key,
                    Body=manifest_data.encode('utf-8'),
                    ContentType='application/json'
                )
                logger.info(f"Updated version manifest at {manifest_key}")
        
        except Exception as e:
            logger.warning(f"Failed to create version manifest: {e}")


class S3ModelDownloader:
    """Handles downloading models for inference from S3."""
    
    def __init__(self, bucket: str, region: str = 'us-west-2', profile: Optional[str] = None):
        """
        Initialize S3 downloader.
        
        Args:
            bucket: S3 bucket name
            region: AWS region
            profile: AWS profile name (optional)
        """
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.s3_client = session.client('s3', region_name=region)
        self.bucket = bucket
    
    def download_model(self, s3_key: str, local_path: Path) -> None:
        """
        Download model from S3 to local path.
        
        Args:
            s3_key: S3 key for the model
            local_path: Local path to save model
        
        Raises:
            ClientError if download fails
        """
        logger.info(f"Downloading model from s3://{self.bucket}/{s3_key} to {local_path}")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.s3_client.download_file(self.bucket, s3_key, str(local_path))
            logger.info(f"Successfully downloaded model to {local_path}")
        except ClientError as e:
            logger.error(f"Failed to download model from S3: {e}")
            raise
    
    def download_metadata(self, s3_key: str) -> dict:
        """
        Download and parse metadata JSON from S3.
        
        Args:
            s3_key: S3 key for the metadata
        
        Returns:
            Dictionary with metadata
        
        Raises:
            ClientError if download fails
        """
        logger.info(f"Downloading metadata from s3://{self.bucket}/{s3_key}")
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            metadata = json.load(response['Body'])
            return metadata
        except ClientError as e:
            logger.error(f"Failed to download metadata from S3: {e}")
            raise
