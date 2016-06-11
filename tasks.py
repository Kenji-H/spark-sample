# -*- coding: utf-8 -*-

import os
import json
from invoke import task

APP_HOME = os.path.dirname(os.path.abspath(__file__))
CONF_FILE = os.path.join(APP_HOME, "conf/dev.json")
CONF = json.loads(open(CONF_FILE).read())

os.environ["AWS_ACCESS_KEY_ID"] = CONF["aws"]["credentials"]["access_key_id"]
os.environ["AWS_SECRET_ACCESS_KEY"] = CONF["aws"]["credentials"]["secret_access_key"]
os.environ["AWS_DEFAULT_REGION"] = CONF["aws"]["config"]["region"]
os.environ["AWS_DEFAULT_OUTPUT"] = CONF["aws"]["config"]["output"]

@task
def compile(ctx):
    ctx.run("sbt compile")

@task
def test(ctx):
    ctx.run("sbt test")
    
@task
def assembly(ctx):
    ctx.run("sbt assembly")

@task
def emr_create_cluster(ctx):
    cmd_template = 'aws emr create-cluster ' +\
        '--name "%s" ' +\
        '--release-label %s ' +\
        '--applications Name=Spark ' +\
        '--ec2-attributes KeyName=%s ' +\
        '--instance-type %s ' +\
        '--instance-count %s ' +\
        '--use-default-roles'
    cluster_name = CONF["aws"]["emr"]["cluster-name"] 
    release_label = CONF["aws"]["emr"]["release-label"] 
    key_name = CONF["aws"]["emr"]["key-name"] 
    instance_type = CONF["aws"]["emr"]["instance-type"] 
    instance_count = CONF["aws"]["emr"]["instance-count"]

    cmd = cmd_template % (cluster_name, release_label, key_name, instance_type, instance_count)
    ctx.run(cmd)

@task
def emr_terminate_cluster(ctx, cluster_id):
    cmd_template = 'aws emr terminate-clusters --cluster-ids %s'
    cmd = cmd_template % cluster_id
    ctx.run(cmd)
