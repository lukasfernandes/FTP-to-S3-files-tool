import itertools
from io import BytesIO
from ftplib import FTP

import random
import boto3
import concurrent.futures
import threading
import logging
import time
import datetime
import mimetypes

class Result:
    def __init__(self, directory, files_total, copied_total, failed_total, failed_files):
        self.directory = directory
        self.files_total = files_total
        self.copied_total = copied_total
        self.failed_total = failed_total
        self.failed_files = failed_files


class FTPToS3:
    def __init__(self, ftp_host, ftp_login, ftp_password, bucket, aws_access_key_id, aws_secret_access_key, region_name,
                 num_threads=1, log_level="info"):
        self.logger = None
        self.s3_client = None
        self.ftp_directory_path = None
        self.ftp_host = ftp_host
        self.ftp_login = ftp_login
        self.ftp_password = ftp_password
        self.s3_bucket = bucket
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.num_threads = num_threads
        self.init_logging(log_level)
        self.connect_to_s3()
        self.output_prefix = ""
        self.result = []
        self.threads_counter = 0
        self.files_copied = 0
        self.initial_date = datetime.datetime.now()

    def copy_speed(self):
        return round(self.files_copied / int((datetime.datetime.now() - self.initial_date).seconds), 1)

    def connect_to_s3(self):
        try:
            self.s3_client = boto3.client('s3',
                                          aws_access_key_id=self.aws_access_key_id,
                                          aws_secret_access_key=self.aws_secret_access_key,
                                          region_name=self.region_name)
        except:
            self.logger.error("Failed to establish AWS S3 client")

    def init_logging(self, log_level):
        self.logger = logging.getLogger("ftp_to_s3")
        level = getattr(logging, log_level.upper(), 20)
        self.logger.setLevel(level)

        file_handler = logging.FileHandler("files_log.log")
        console_handler = logging.StreamHandler()
        file_handler.setLevel(level)
        console_handler.setLevel(level)

        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def copy_ftp_to_bucket(self, ftp_directory_path, output_prefix=""):

        self.output_prefix = output_prefix

        self.ftp_directory_path = ftp_directory_path

        self.copy_files(ftp_directory_path, output_prefix)

        self.logger.info(f"Total speed: {self.copy_speed()} files per seconds")
        self.logger.info(f"The total of directory: {len(self.result)}")
        self.logger.info(f"The expected number of files copied: {sum(obj.files_total for obj in list(self.result))}")
        self.logger.info(f"The actual number of files copied: {sum(obj.copied_total for obj in list(self.result))}")
        self.logger.info(f"The total of failed files: {sum(obj.failed_total for obj in list(self.result))}")
        self.logger.info("Check the files_log.log file for more information")

        return self.result

    def num_thread_add(self):
        self.threads_counter = self.threads_counter + 1
        return None

    def num_thread_remove(self):
        self.threads_counter = self.threads_counter - 1
        return None

    def copy_files(self, ftp_directory_path, output_prefix=""):
        filenames = []
        ftp_local_connection = object

        for i in range(20):
            try:
                ftp_local_connection = self.ftp_connect(ftp_directory_path)
                ftp_local_connection.cwd(ftp_directory_path)
                filenames = ftp_local_connection.nlst()
                filenames = [f'{ftp_directory_path}/{f}' for f in filenames]
                break
            except Exception as e:
                time.sleep(random.uniform(0.1, 0.5))
                self.logger.info(f"{ftp_directory_path} -- Retry {i}: failed to list files from FTP server")
                if i == 19:
                    self.logger.error(f"Directory: {ftp_directory_path} - Failed to list files from FTP server.")
                    self.logger.error(f"{ftp_directory_path}: {str(e)}")
                    return None

        num_threads = self.num_threads
        if num_threads > len(filenames) > 0:
            num_threads = len(filenames)

        while (self.threads_counter + num_threads) > (self.num_threads + 1):
            time.sleep(random.uniform(0.1, 1.5))

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as self_executor:
            results = self_executor.map(self.copy_file, filenames, itertools.repeat(output_prefix, len(filenames)))

        result_list = list(results)
        folders = sum(1 for i in result_list if i == "folder")
        copied = sum(1 for i in result_list if i == "copied")
        failed = list(filter(lambda x: x != "folder" and x != "copied", result_list))

        self.logger.info(f"Speed: {self.copy_speed()} files per seconds")
        self.logger.info(f"Directory: {ftp_directory_path}")
        self.logger.info(f"Files Total: {(len(filenames) - folders)}")
        self.logger.info(f"Copied Total: {copied}")
        self.logger.info(f"Failed Total: {len(failed)}")
        self.logger.info(f"Failed Files: {len(failed)}")

        self.result.append(Result(ftp_directory_path, (len(filenames) - folders), copied, len(failed), failed))

        self.close_local_ftp_conn(ftp_local_connection, ftp_directory_path)
        return None

    def copy_file(self, ftp_filename, output_prefix):
        self.num_thread_add()
        ftp_command = "RETR " + ftp_filename
        curr_file_buffer = BytesIO()
        ftp_local_connection = object

        for i in range(20):
            try:
                ftp_local_connection = self.ftp_connect(ftp_filename)
                ftp_local_connection.retrbinary(ftp_command, curr_file_buffer.write)
                break
            except Exception as e:

                if str(e).__contains__("Is a directory"):
                    self.num_thread_remove()
                    self.logger.info(f"{ftp_filename} is a directory")

                    while self.threads_counter > (self.num_threads + 1):
                        time.sleep(0.01)

                    th = threading.Thread(target=self.copy_files(ftp_filename,
                                                                 f'{self.output_prefix}{(ftp_filename.replace(self.ftp_directory_path, ""))}'))
                    th.start()

                    return "folder"
                else:
                    time.sleep(random.uniform(0.1, 0.5))
                    self.logger.info(f"{ftp_filename} -- Retry {i}: failed to read file from FTP server")
                    if i == 19:
                        self.logger.error(f"{ftp_filename}: Failed to read file from FTP server.")
                        self.logger.error(f"{ftp_filename}: {str(e)}")
                        self.num_thread_remove()
                        return ftp_filename

        curr_file_buffer.seek(0)

        filename = ftp_filename.split("/")[-1]

        if self.output_prefix == "" and output_prefix == "":
            s3_output = filename
        else:
            if self.output_prefix == "":
                s3_output = f"{output_prefix[1:]}/{filename}"
            else:
                s3_output = f"{output_prefix}/{filename}"

        for i in range(20):
            try:
                mime_type = mimetypes.guess_type(ftp_filename)

                if mime_type and mime_type[0] is not None:
                    self.s3_client.upload_fileobj(curr_file_buffer, self.s3_bucket, s3_output,
                                                      ExtraArgs={
                                                          'CacheControl': 'public,max-age=86400',
                                                          'ACL': 'public-read',
                                                          'ContentType': mime_type[0],
                                                      })
                else:
                    self.s3_client.upload_fileobj(curr_file_buffer, self.s3_bucket, s3_output,
                                                  ExtraArgs={
                                                      'CacheControl': 'public,max-age=86400',
                                                      'ACL': 'public-read',
                                                  })

                self.logger.info(f"{ftp_filename} -- uploaded file to S3 Server: {s3_output}")
                self.files_copied = self.files_copied + 1
                break
            except Exception as e:
                time.sleep(random.uniform(0.1, 0.5))
                self.logger.info(f"{ftp_filename} -- Retry {i}: Failed to upload file to S3 Server: {s3_output}")
                if i == 19:
                    self.logger.error(f"{ftp_filename}: Failed to upload file to S3 Server: {s3_output}")
                    self.logger.error(f"{ftp_filename} -- {str(e)}")
                    self.close_local_ftp_conn(ftp_local_connection, ftp_filename)
                    self.num_thread_remove()
                    return ftp_filename

        self.close_local_ftp_conn(ftp_local_connection, ftp_filename)
        self.num_thread_remove()
        return "copied"

    def close_local_ftp_conn(self, local_conn, ftp_filename):
        if local_conn is not None:
            try:
                local_conn.quit()
            except Exception as e:
                self.logger.debug(f"{ftp_filename} -- failed to close FTP connection")
                self.logger.debug(f"{ftp_filename} -- {str(e)}")
                pass

    def ftp_connect(self, ftp_filename):
        try:
            local_ftp_conn = FTP(self.ftp_host, timeout=60)
        except Exception as e:
            self.logger.error(f"{ftp_filename} -- failed to establish local FTP connection")
            self.logger.error(f"{ftp_filename} -- {str(e)}")
            return None

        try:
            local_ftp_conn.login(self.ftp_login, self.ftp_password)
            return local_ftp_conn
        except Exception as e:
            self.logger.error(f"{ftp_filename} -- failed to log in to FTP server")
            self.logger.error(f"{ftp_filename} -- {str(e)}")
            self.close_local_ftp_conn(local_ftp_conn, ftp_filename)
            return None