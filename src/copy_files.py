from ftp_to_s3 import FTPToS3

ftp_host = ""
ftp_login = ""
ftp_password = ""
s3_bucket_name = ""
num_threads = 16
log_level = "info"
aws_access_key_id = ""
aws_secret_access_key = ""
region_name = "us-east-1"
ftp_path = "/img"
s3_output_path = "img"

ftp_to_s3 = FTPToS3(ftp_host, ftp_login, ftp_password, s3_bucket_name, aws_access_key_id, aws_secret_access_key, region_name, num_threads=num_threads, log_level=log_level)

result = ftp_to_s3.copy_ftp_to_bucket(ftp_path, s3_output_path)

print(f"The total of directory: {len(result)}")
print(f"The expected number of files copied: { sum(obj.files_total for obj in list(result))}")
print(f"The actual number of files copied: { sum(obj.copied_total for obj in list(result))}")
print(f"The total of failed files: {sum(obj.failed_total for obj in list(result))}")
print("Check the files_log.log file for more information")
