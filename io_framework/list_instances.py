#!/usr/bin/env python3
import boto3

# Currently prints id, private ip, public ip of each ec2 instance
# can be used to return necessary information about the ec2 instance in aws - can add more functions if necessary

def list_instances():
    ec2 = boto3.resource('ec2')
    for instance in ec2.instances.all():
        i = ec2.Instance(id=instance.id)
        print("Instance ID: ", i.id, "Private IP Address: ", i.private_ip_address, "Public IP Address: ", i.public_ip_address)
        return i.id

# lists all the security groups

def list_security_groups():
    ec2 = boto3.client('ec2')
    security_groups = ec2.describe_security_groups()
    for security_group in security_groups["SecurityGroups"]:
        print("Group Name: ", security_group["GroupName"], "IP Permissions: ", security_group["IpPermissions"], "\n")

# this function creates a security group with given ip permissions (more aspects of the security group can be edited as well)

def create_security_group(group_name, group_description, from_port, source, description, to_port):
    ec2 = boto3.client('ec2')
    response = ec2.create_security_group(GroupName=group_name, Description=group_description)
    security_group_id = response['GroupId']
    ec2.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': from_port,
                'IpRanges': [
                    {
                        'CidrIp': source,
                        'Description': description
                    }
                ],
                'ToPort': to_port
            }
        ])


