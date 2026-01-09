#!/usr/bin/env python3
"""
Transfer IGVF files directly from S3 to GCS using gcloud transfer jobs.
This avoids downloading files locally, saving disk space and bandwidth.
"""

import os
import requests
import pandas as pd
import argparse
import json
import subprocess
from requests.auth import HTTPBasicAuth
from tqdm import tqdm


SEQUENCE_FILE_COLUMNS = ['R1_path', 'R2_path']
CONFIGURATION_FILE_COLUMNS = ['seqspec']
TABULAR_FILE_COLUMNS = ['barcode_onlist', 'guide_design', 'barcode_hashtag_map']

# Helper class for colored console output
class Color:
    """A simple class to add color to terminal output."""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'


def get_auth(keypair_path=None):
    if keypair_path:
        with open(keypair_path) as f:
            keypair = json.load(f)
            return HTTPBasicAuth(keypair['key'], keypair['secret'])

    key = os.getenv('IGVF_API_KEY')
    secret = os.getenv('IGVF_SECRET_KEY')
    if key and secret:
        return HTTPBasicAuth(key, secret)

    raise RuntimeError('No credentials provided. Set IGVF_API_KEY and IGVF_SECRET_KEY or provide a keypair JSON.')


def colored_print(color, message):
    """Prints a message in the specified color."""
    print(f"{color}{message}{Color.END}")


def get_s3_uri_from_portal(accession: str, auth: HTTPBasicAuth, file_type: str):
    """
    Query the IGVF portal to get the S3 URI for a file.
    
    Args:
        accession: File accession (e.g., IGVFFI1234ABCD)
        auth: HTTPBasicAuth object
        file_type: 'sequence', 'tabular', or 'configuration'
    
    Returns:
        dict with 's3_uri', 'filename', 'md5sum', 'file_size' or None if not found
    """
    file_type_to_route = {
        'sequence': 'sequence-files',
        'tabular': 'tabular-files',
        'configuration': 'configuration-files'
    }
    
    if file_type not in file_type_to_route:
        raise ValueError(f"Unknown file_type '{file_type}'")
    
    route = file_type_to_route[file_type]
    url = f"https://api.data.igvf.org/{route}/{accession}/@@object?format=json"
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        data = response.json()
        
        s3_uri = data.get('s3_uri')
        if not s3_uri:
            colored_print(Color.RED, f"No s3_uri found for {accession}")
            return None
        
        # Extract the S3 path components
        # s3_uri format: s3://bucket/path/to/file.ext
        return {
            's3_uri': s3_uri,
            'filename': os.path.basename(s3_uri),
            'md5sum': data.get('content_md5sum', ''),
            'file_size': data.get('file_size', 0),
            'accession': accession
        }
    except Exception as e:
        colored_print(Color.RED, f"Failed to get S3 URI for {accession}: {e}")
        return None


def create_role_arn_file(role_arn: str, output_path: str = 'role-arn.json'):
    """Create the role-arn.json file needed for gcloud transfer."""
    with open(output_path, 'w') as f:
        json.dump({'roleArn': role_arn}, f, indent=2)
    return output_path


def create_transfer_job(s3_info: dict, gcs_bucket: str, gcs_prefix: str, 
                       role_arn_file: str, gcp_project: str, dry_run: bool = False):
    """
    Create a gcloud transfer job to move file from S3 to GCS.
    
    Args:
        s3_info: Dict with s3_uri, filename, etc.
        gcs_bucket: Destination GCS bucket name
        gcs_prefix: Destination prefix/folder in GCS
        role_arn_file: Path to role-arn.json
        gcp_project: GCP project ID
        dry_run: If True, only print commands without executing
    
    Returns:
        GCS URI of the destination file
    """
    # Parse S3 URI
    s3_uri = s3_info['s3_uri']
    accession = s3_info['accession']
    filename = s3_info['filename']
    
    # Extract bucket and path from s3://bucket/path/to/file
    s3_parts = s3_uri.replace('s3://', '').split('/', 1)
    s3_bucket = s3_parts[0]
    s3_path = s3_parts[1] if len(s3_parts) > 1 else ''
    
    # Source and destination
    src = f"s3://{s3_bucket}/"
    dest = f"gs://{gcs_bucket}/{gcs_prefix}/"
    include_prefix = s3_path
    
    # Job name (must be unique and follow naming rules)
    job_name = f"transferJobs/{accession}"
    
    # Build gcloud command
    cmd = [
        'gcloud', 'transfer', 'jobs', 'create',
        src, dest,
        '--name', job_name,
        '--description', f"Transfer {accession}",
        '--source-creds-file', role_arn_file,
        '--include-prefixes', include_prefix,
        '--overwrite-when', 'never',
        '--project', gcp_project
    ]
    
    if dry_run:
        colored_print(Color.YELLOW, f"[Dry run] Would execute: {' '.join(cmd)}")
        return f"gs://{gcs_bucket}/{gcs_prefix}/{filename}"
    
    try:
        colored_print(Color.GREEN, f"Creating transfer job for {accession}...")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        colored_print(Color.GREEN, f"Transfer job created: {job_name}")
        
        # Return the destination GCS URI
        gcs_uri = f"gs://{gcs_bucket}/{gcs_prefix}/{filename}"
        return gcs_uri
        
    except subprocess.CalledProcessError as e:
        colored_print(Color.RED, f"Failed to create transfer job for {accession}")
        colored_print(Color.RED, f"Error: {e.stderr}")
        return None


def process_sample_sheet_s3_transfer(df, auth: HTTPBasicAuth, args, 
                                     file_types='all') -> pd.DataFrame:
    """
    For each IGVFFI accession, get S3 URI from portal and create transfer job to GCS.
    Updates the dataframe with GCS paths.
    """
    
    cols_to_update = []
    if file_types in ['fastq', 'all']:
        cols_to_update.extend(SEQUENCE_FILE_COLUMNS)
    if file_types in ['other', 'all']:
        cols_to_update.extend(CONFIGURATION_FILE_COLUMNS)
        cols_to_update.extend(TABULAR_FILE_COLUMNS)

    col_to_file_type = {}
    for col in SEQUENCE_FILE_COLUMNS:
        col_to_file_type[col] = 'sequence'
    for col in CONFIGURATION_FILE_COLUMNS:
        col_to_file_type[col] = 'configuration'
    for col in TABULAR_FILE_COLUMNS:
        col_to_file_type[col] = 'tabular'

    # Cache: accession -> GCS path
    resolved_path_cache = {}

    # Create role-arn file
    role_arn_file = create_role_arn_file(args.aws_role_arn)
    colored_print(Color.GREEN, f"Created {role_arn_file}")

    # Set GCP project
    if not args.dry_run:
        subprocess.run(['gcloud', 'config', 'set', 'project', args.gcp_project], 
                      check=True, capture_output=True)

    # Walk rows and create transfer jobs
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="Creating transfer jobs"):
        for col in cols_to_update:
            if col not in df.columns:
                continue
            value = row[col]

            # Only process IGVF file accessions
            if not (pd.notna(value) and isinstance(value, str) and value.startswith("IGVFFI")):
                continue

            accession = value
            
            # Check cache
            if accession in resolved_path_cache:
                df.at[idx, col] = resolved_path_cache[accession]
                continue

            # Get S3 URI from portal
            file_type = col_to_file_type[col]
            s3_info = get_s3_uri_from_portal(accession, auth, file_type)
            
            if not s3_info:
                colored_print(Color.RED, f"Skipping {accession} - could not get S3 info")
                continue

            # Create transfer job
            gcs_uri = create_transfer_job(
                s3_info, 
                args.gcs_bucket, 
                args.gcs_prefix,
                role_arn_file,
                args.gcp_project,
                dry_run=args.dry_run
            )

            if gcs_uri:
                resolved_path_cache[accession] = gcs_uri
                df.at[idx, col] = gcs_uri

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Transfer IGVF files directly from S3 to GCS using gcloud transfer jobs.',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Example usage:

# Transfer all files from IGVF S3 to GCS
python3 s3_to_gcs_transfer.py \\
  --sample IGVFDS5057HJKP_metadata.tsv \\
  --keypair igvf_key.json \\
  --gcs-bucket igvf-pertub-seq-pipeline-data \\
  --gcs-prefix sr-data/engreitz_benchmark \\
  --gcp-project igvf-pertub-seq-pipeline \\
  --aws-role-arn arn:aws:iam::407227577691:role/S3toPertubSeqGoogleCloudTransfer

# Dry run to see what would be transferred
python3 s3_to_gcs_transfer.py \\
  --sample IGVFDS5057HJKP_metadata.tsv \\
  --keypair igvf_key.json \\
  --gcs-bucket igvf-pertub-seq-pipeline-data \\
  --gcs-prefix sr-data/engreitz_benchmark \\
  --gcp-project igvf-pertub-seq-pipeline \\
  --aws-role-arn arn:aws:iam::407227577691:role/S3toPertubSeqGoogleCloudTransfer \\
  --dry-run
        """
    )
    
    parser.add_argument('--sample', required=True,
                        help='Path to the TSV file containing sample information.')
    parser.add_argument('--keypair', required=True,
                        help='Path to JSON file containing IGVF-API key pair.')
    
    parser.add_argument('--gcs-bucket', required=True,
                        help='Destination GCS bucket name (e.g., igvf-pertub-seq-pipeline-data)')
    parser.add_argument('--gcs-prefix', required=True,
                        help='Destination folder in GCS (e.g., sr-data/engreitz_benchmark)')
    
    parser.add_argument('--gcp-project', required=True,
                        help='GCP project ID (e.g., igvf-pertub-seq-pipeline)')
    parser.add_argument('--aws-role-arn', required=True,
                        help='AWS IAM role ARN for S3 access (e.g., arn:aws:iam::407227577691:role/S3toPertubSeqGoogleCloudTransfer)')
    
    parser.add_argument('--file-types', choices=['fastq', 'other', 'all'], default='all',
                        help='Which file types to transfer: fastq, other, or all (default)')
    
    parser.add_argument('--dry-run', action='store_true',
                        help='Print transfer commands without executing them')

    args = parser.parse_args()

    try:
        colored_print(Color.GREEN, "Starting S3 to GCS transfer process...")
        
        # Get authentication
        auth = get_auth(args.keypair)

        # Read sample sheet
        df = pd.read_csv(args.sample, sep='\t')

        # Process transfers
        updated_df = process_sample_sheet_s3_transfer(
            df, auth, args, file_types=args.file_types
        )

        # Save updated TSV with GCS paths
        output_basename = os.path.basename(args.sample)
        updated_paths_filename = f"gcs_paths_{output_basename}"
        updated_df.to_csv(updated_paths_filename, sep='\t', index=False)

        colored_print(Color.GREEN, f"\nTransfer jobs created successfully!")
        colored_print(Color.GREEN, f"Updated sample sheet saved: {updated_paths_filename}")
        colored_print(Color.YELLOW, "\nNote: Transfer jobs are asynchronous. Monitor progress with:")
        colored_print(Color.YELLOW, f"  gcloud transfer jobs list --project={args.gcp_project}")
        colored_print(Color.YELLOW, f"  gcloud transfer operations list --project={args.gcp_project}")

    except Exception as e:
        colored_print(Color.RED, f"[Error] {e}")
        raise