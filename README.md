# read-books
Crawls remote web page

###Installation

This requires Python3
You will need to import the requirements.txt by running

`pip install -r requirements.txt`

You will then need to configure aws cli by running 
```
aws configure
```
from your command line, and type the aws access id and secret.
Next, you will need to setup crontab by running 

```
sudo crontab -e
```
and paste the below in the vi editor 
```
*/5 * * * * python /PATH_TO_DOWNLOADED/lavamap/main.py >> /PATH_TO_DOWNLOADED/lavamap/log.out 2>&1
```

Note: If your crontab runs as a different user e.g root, You may want to `sudo su root` and run `aws configure` as that user.

