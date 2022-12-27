# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from constructs import Construct
from aws_cdk import NestedStack
from aws_cdk.aws_ec2 import GatewayVpcEndpointAwsService, SubnetSelection, SubnetType, Vpc, InterfaceVpcEndpointAwsService
from aws_cdk.aws_glue_alpha import Database
from aws_cdk.aws_iam import Group, Role
from aws_analytics_reference_architecture import DataLakeCatalog, DataLakeStorage, LakeFormationAdmin, LakeFormationS3Location
from common_cdk.audit_trail_glue import AuditTrailGlue

from common.common_cdk.auto_empty_bucket import AutoEmptyBucket
from common.common_cdk.config import AutoEmptyConfig


class DataLakeFoundations(NestedStack):

    @property
    def raw_s3_bucket(self):
        return self.__raw_s3_bucket

    @property
    def clean_s3_bucket(self):
        return self.__clean_s3_bucket

    @property
    def curated_s3_bucket(self):
        return self.__curated_s3_bucket

    @property
    def raw_glue_db(self):
        return self.__raw_glue_db

    @property
    def clean_glue_db(self):
        return self.__clean_glue_db

    @property
    def curated_glue_db(self):
        return self.__curated_glue_db

    @property
    def audit_glue_db(self):
        return self.__audit_glue_db

    @property
    def logs_s3_bucket(self):
        return self.__logs_s3_bucket

    @property
    def vpc(self):
        return self.__vpc

    @property
    def private_subnets_selection(self):
        return self.__private_subnets

    @property
    def public_subnets_selection(self):
        return self.__public_subnets

    @property
    def admin_group(self):
        return self.__admin_group

    @property
    def analysts_group(self):
        return self.__analysts_group

    @property
    def developers_group(self):
        return self.__developers_group
    
    @property
    def lf_admin_role(self):
        return self.__lf_admin_role

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

          # implement the glue data catalog databases used in the data lake
        catalog = DataLakeCatalog(self, 'DataLakeCatalog')
        self.__raw_glue_db = catalog.raw_database
        self.__clean_glue_db = catalog.clean_database
        self.__curated_glue_db = catalog.transform_database
        self.__audit_glue_db = Database(self, 'AuditGlueDB', database_name='ara_audit_data_' + self.account)

        # implement the S3 buckets for the data lake
        storage = DataLakeStorage(self, 'DataLakeStorage')
        self.__logs_s3_bucket = AutoEmptyBucket(
            self, 'Logs',
            bucket_name='ara-logs-' + self.account,
            uuid=AutoEmptyConfig.FOUNDATIONS_UUID
        ).bucket

        self.__raw_s3_bucket = storage.raw_bucket
        self.__clean_s3_bucket = storage.clean_bucket
        self.__curated_s3_bucket = storage.transform_bucket

        AuditTrailGlue(self, 'GlueAudit',
            log_bucket=self.__logs_s3_bucket,
            audit_bucket=self.__curated_s3_bucket,
            audit_db=self.__audit_glue_db,
            audit_table=self.__curated_s3_bucket.bucket_name
        )

        self.__raw_glue_db.location_uri = 's3:////'+ self.__raw_s3_bucket.bucket_name


        # implement lake formation permissions
        # lfAdmin = LakeFormationAdmin(self,'DataLakeAdmin')
        # self.__lf_admin_role = Role(self, 'lf-admin-role',))
        LakeFormationS3Location(self,'S3LocationRaw', 
            s3_location={
                "bucket_name": self.__raw_s3_bucket.bucket_name,
                "object_key": ""
                },
            kms_key_id= self.__raw_s3_bucket.encryption_key.key_id
        )
        LakeFormationS3Location(self,'S3LocationClean', 
            s3_location={
                "bucket_name": self.__clean_s3_bucket.bucket_name,
                "object_key": ""
                },
            kms_key_id= self.__clean_s3_bucket.encryption_key.key_id
        )
        LakeFormationS3Location(self,'S3LocationTransform', 
            s3_location={
                "bucket_name": self.__curated_s3_bucket.bucket_name,
                "object_key": ""
                },
            kms_key_id= self.__curated_s3_bucket.encryption_key.key_id
        )

        # Below code gives error 'NoneType' object has no attribute 'key_id'
        # LakeFormationS3Location(self,'S3LocationAudit', 
        #     s3_location={
        #         "bucket_name": self.__logs_s3_bucket.bucket_name,
        #         "object_key": ""
        #         },
        #     kms_key_id= self.__logs_s3_bucket.encryption_key.key_id
        # )


        # the vpc used for the overall data lake (same vpc, different subnet for modules)
        self.__vpc = Vpc(self, 'Vpc')
        self.__public_subnets = self.__vpc.select_subnets(subnet_type=SubnetType.PUBLIC)
        self.__private_subnets = self.__vpc.select_subnets(subnet_type=SubnetType.PRIVATE_WITH_NAT)
        self.__vpc.add_gateway_endpoint("S3GatewayEndpoint",
                                        service=GatewayVpcEndpointAwsService.S3,
                                        subnets=[SubnetSelection(subnet_type=SubnetType.PUBLIC),
                                                 SubnetSelection(subnet_type=SubnetType.PRIVATE_WITH_NAT)])

        # IAM groups
        self.__admin_group = Group(self, 'GroupAdmins', group_name='ara-admins')
        self.__analysts_group = Group(self, 'GroupAnalysts', group_name='ara-analysts')
        self.__developers_group = Group(self, 'GroupDevelopers', group_name='ara-developers')

       
