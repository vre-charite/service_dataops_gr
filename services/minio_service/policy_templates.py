import uuid

# admin will have all the permission to access 
# all the name folder under the bucket
def create_admin_policy(project_code):
    template = '''
    {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::gr-%s", "arn:aws:s3:::core-%s"]
            },
            {
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::gr-%s/*", "arn:aws:s3:::core-%s/*"]
            }
        ]
    }
    '''%(project_code, project_code, project_code, project_code)

    # now create the template file since we need to use the file
    # with minio admin client to create policy
    # since here we will write to disk. to avoid collision use the uuid4
    template_name = str(uuid.uuid4())+".json"
    policy_file = open(template_name, "w")
    policy_file.write(template)
    policy_file.close()

    return template_name


# collaborator will have all the permission to access 
# only the name folder under the bucket for greenroom
# but full access in the core
def create_collaborator_policy(project_code):
    template = '''
    {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::gr-%s", "arn:aws:s3:::core-%s"]
            },
            {
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::gr-%s/${jwt:preferred_username}/*", "arn:aws:s3:::core-%s/*"]
            }
        ]
    }
    '''%(project_code, project_code, project_code, project_code)

    # now create the template file since we need to use the file
    # with minio admin client to create policy
    # since here we will write to disk. to avoid collision use the uuid4
    template_name = str(uuid.uuid4())+".json"
    policy_file = open(template_name, "w")
    policy_file.write(template)
    policy_file.close()

    return template_name


def create_contributor_policy(project_code):
    template = '''
    {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::gr-%s", "arn:aws:s3:::core-%s"]
            },
            {
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::gr-%s/${jwt:preferred_username}/*", "arn:aws:s3:::core-%s/${jwt:preferred_username}/*"]
            }
        ]
    }
    '''%(project_code, project_code, project_code, project_code)

    # now create the template file since we need to use the file
    # with minio admin client to create policy
    # since here we will write to disk. to avoid collision use the uuid4
    template_name = str(uuid.uuid4())+".json"
    policy_file = open(template_name, "w")
    policy_file.write(template)
    policy_file.close()

    return template_name
