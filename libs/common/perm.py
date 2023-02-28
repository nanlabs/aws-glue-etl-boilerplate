import wget

pem_file_url = "https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem"
output_directory = "/tmp"


def load_tls_ca_bundle():
    """Load the TLS CA bundle from the RDS website and save it to a file."""
    pem_file = wget.download(pem_file_url, out=output_directory)
    return pem_file
