# FTP to S3 Copy Tool
This is a utility written in python to copy files from an FTP server to an AWS S3 bucket. 

## Usage Notes

### AWS Client
The boto3 library is used to upload files to the specified S3 bucket.

### FTP Client
The FTP server is connected to using `ftplib` from the Python standard library. 

### Directory
This was origingally written to copy all of the files from a given directory on the FTP server,  Even if there are folders inside the directory, they will also be copied.

### Parallel Execution
The copy of all files from a given directory can be parallelized. The number of threads to run with can be configured. The biggest contributor to latency in the transfer process is IO, so multithreading can significantly improve the performance of the transfer of many files.

### Retries
If any files in the given directory failed to be copied for some reason, there will be two retry.

### How to run
Script:
```    
    .\venv\Scripts\activate.bat
    cd..
    cd..
    python .\src\copy_files.py    
```
