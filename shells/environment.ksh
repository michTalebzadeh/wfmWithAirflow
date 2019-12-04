#!/bin/ksh
#
# Program:      environment.ksh
# Type:         Generic
# Description:  environment file for hadoop
#               environment has been defined for their execution.
# Author:       Mich Talebzadeh
# Version:      1.0
#
# Modified:     
#
###################################
# Modify the following lines only

export TZ=GB

export JAVA_HOME=/usr/java/latest
export PATH=$PATH:$JAVA_HOME/bin
GENERIC_ROOT=/home/hduser/dba
SPECIFIC_ROOT=/home/hduser/dba

export GEN_APPSDIR=$GENERIC_ROOT/bin
export GEN_LOGDIR=$GENERIC_ROOT/log
export GEN_ETCDIR=$GENERIC_ROOT/etc
export GEN_ENVDIR=$GENERIC_ROOT/env
export GEN_ADMINDIR=$GENERIC_ROOT/admin
export PASSFILE=$GENERIC_ROOT/env/.syb_accounts


export ADMINDIR=$SPECIFIC_ROOT/admin
export TMPDIR=$SPECIFIC_ROOT/tmp
export LOGDIR=$SPECIFIC_ROOT/log
export ETCDIR=$SPECIFIC_ROOT/etc
export HTMLDIR=$SPECIFIC_ROOT/html
export APPSDIR=$SPECIFIC_ROOT/bin

ulimit -n 63536
. /home/hduser/.kshrc
