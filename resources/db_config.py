import os

host = 'localhost' if os.environ.get('TRAVIS') == 'TRUE' else 'ec2-34-211-57-101.us-west-2.compute.amazonaws.com'
port = 8086 if os.environ.get('TRAVIS') == 'TRUE' else 8086
username = 'root'
password = 'root'